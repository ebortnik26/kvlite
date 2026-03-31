#include "internal/read_write_delta_hash_table.h"

#include <algorithm>
#include <cstring>

#include "internal/profiling.h"
#include "internal/version_dedup.h"

namespace kvlite {
namespace internal {

ReadWriteDeltaHashTable::ReadWriteDeltaHashTable()
    : ReadWriteDeltaHashTable(Config{}) {}

ReadWriteDeltaHashTable::ReadWriteDeltaHashTable(const Config& config)
    : DeltaHashTable(config),
      multi_version_(1u << config.bucket_bits),
      used_bits_(1u << config.bucket_bits),
      ext_arena_owned_(sizeof(Bucket) + bucketStride(), /*concurrent=*/true) {
    ext_arena_ = &ext_arena_owned_;
    bucket_locks_ = std::make_unique<Spinlock[]>(1u << config.bucket_bits);
}

ReadWriteDeltaHashTable::~ReadWriteDeltaHashTable() = default;

// --- Find (locked) ---

bool ReadWriteDeltaHashTable::findAll(uint64_t hash,
                                       std::vector<uint64_t>& packed_versions,
                                       std::vector<uint32_t>& ids) const {
    uint32_t bi = bucketIndex(hash);
    uint64_t suffix = suffixFromHash(hash);

    SpinlockGuard guard(bucket_locks_[bi]);
    return findAllByHash(bi, suffix, packed_versions, ids);
}

bool ReadWriteDeltaHashTable::findFirst(uint64_t hash,
                                         uint64_t& packed_version,
                                         uint32_t& id) const {
    uint32_t bi = bucketIndex(hash);
    uint64_t suffix = suffixFromHash(hash);

    SpinlockGuard guard(bucket_locks_[bi]);
    return findFirstByHash(bi, suffix, packed_version, id);
}

bool ReadWriteDeltaHashTable::findFirstBounded(uint64_t hash, uint64_t upper_bound,
                                                uint64_t& packed_version,
                                                uint32_t& id) const {
    uint32_t bi = bucketIndex(hash);
    uint64_t suffix = suffixFromHash(hash);

    SpinlockGuard guard(bucket_locks_[bi]);
    return findFirstBoundedByHash(bi, suffix, upper_bound, packed_version, id);
}

bool ReadWriteDeltaHashTable::contains(uint64_t hash) const {
    uint32_t bi = bucketIndex(hash);
    uint64_t suffix = suffixFromHash(hash);

    SpinlockGuard guard(bucket_locks_[bi]);
    return containsByHash(bi, suffix);
}

// --- Add ---

void ReadWriteDeltaHashTable::addEntry(uint64_t hash,
                                        uint64_t packed_version, uint32_t id) {
    addImpl(bucketIndex(hash), suffixFromHash(hash),
            packed_version, id);
}

bool ReadWriteDeltaHashTable::addEntryIsNew(uint64_t hash,
                                              uint64_t packed_version, uint32_t id) {
    return addImpl(bucketIndex(hash), suffixFromHash(hash),
                   packed_version, id);
}

bool ReadWriteDeltaHashTable::removeEntry(uint64_t hash,
                                           uint64_t packed_version, uint32_t id) {
    uint32_t bi = bucketIndex(hash);
    uint64_t suffix = suffixFromHash(hash);

    SpinlockGuard guard(bucket_locks_[bi]);
    bool group_empty = removeFromChain(bi, suffix, packed_version, id);
    size_.fetch_sub(1, std::memory_order_relaxed);
    updateUsedBits(bi);
    return group_empty;
}

bool ReadWriteDeltaHashTable::updateEntryId(uint64_t hash,
                                              uint64_t packed_version,
                                              uint32_t old_id, uint32_t new_id) {
    uint32_t bi = bucketIndex(hash);
    uint64_t suffix = suffixFromHash(hash);

    SpinlockGuard guard(bucket_locks_[bi]);
    bool found = updateIdInChain(bi, suffix, packed_version, old_id, new_id,
        [this](Bucket& bucket) -> Bucket* {
            return createExtension(bucket);
        });
    updateUsedBits(bi);
    return found;
}

void ReadWriteDeltaHashTable::updateUsedBits(uint32_t bi) {
    used_bits_.set(bi, static_cast<uint16_t>(codec_.decodeBucketUsedBits(buckets_[bi])));
}

bool ReadWriteDeltaHashTable::addImpl(uint32_t bi, uint64_t suffix,
                                       uint64_t packed_version, uint32_t id) {
    SpinlockGuard guard(bucket_locks_[bi]);

    bool is_new = addToChain(bi, suffix, packed_version, id,
        [this](Bucket& bucket) -> Bucket* {
            return createExtension(bucket);
        });
    size_.fetch_add(1, std::memory_order_relaxed);
    if (!is_new) multi_version_.set(bi);
    updateUsedBits(bi);
    return is_new;
}

size_t ReadWriteDeltaHashTable::addEntriesBatch(
        const HashVersionPair* entries, size_t count, uint32_t id) {
    size_t new_keys = 0;
    size_t i = 0;
    while (i < count) {
        uint32_t bi = bucketIndex(entries[i].hash);
        SpinlockGuard guard(bucket_locks_[bi]);

        size_t j = i;
        while (j < count && bucketIndex(entries[j].hash) == bi) {
            bool is_new = addToChain(bi, suffixFromHash(entries[j].hash),
                entries[j].packed_version, id,
                [this](Bucket& bucket) -> Bucket* {
                    return createExtension(bucket);
                });
            if (is_new) ++new_keys;
            else multi_version_.set(bi);
            ++j;
        }
        size_.fetch_add(j - i, std::memory_order_relaxed);
        updateUsedBits(bi);
        i = j;
    }
    return new_keys;
}

// --- Pruning ---

// Gather all keys from a bucket chain, merging entries that span extensions.
std::vector<KeyEntry> ReadWriteDeltaHashTable::gatherBucketKeys(uint32_t bi) const {
    std::vector<KeyEntry> all_keys;
    const Bucket* b = &buckets_[bi];
    while (b) {
        auto contents = codec_.decodeBucket(*b);
        for (auto& key : contents.keys) {
            auto it = std::lower_bound(all_keys.begin(), all_keys.end(),
                key.suffix, [](const KeyEntry& e, uint64_t s) {
                    return e.suffix < s;
                });
            if (it != all_keys.end() && it->suffix == key.suffix) {
                it->packed_versions.insert(it->packed_versions.end(),
                    key.packed_versions.begin(), key.packed_versions.end());
                it->ids.insert(it->ids.end(),
                    key.ids.begin(), key.ids.end());
            } else {
                all_keys.insert(it, std::move(key));
            }
        }
        b = nextBucket(*b);
    }
    // Sort each key's versions descending (may be unsorted after merge).
    for (auto& key : all_keys) {
        if (key.packed_versions.size() > 1) {
            // Sort (pv, id) pairs by pv descending.
            std::vector<std::pair<uint64_t, uint32_t>> pairs(key.packed_versions.size());
            for (size_t i = 0; i < pairs.size(); ++i) {
                pairs[i] = {key.packed_versions[i], key.ids[i]};
            }
            std::sort(pairs.begin(), pairs.end(),
                      [](const auto& a, const auto& b) { return a.first > b.first; });
            for (size_t i = 0; i < pairs.size(); ++i) {
                key.packed_versions[i] = pairs[i].first;
                key.ids[i] = pairs[i].second;
            }
        }
    }

    return all_keys;
}

// Dedup one key's versions against snapshot observation points.
// Returns number of entries removed. Updates key in place.
static size_t dedupKeyVersions(KeyEntry& key,
                               const std::vector<uint64_t>& snapshot_versions) {
    size_t n = key.packed_versions.size();
    std::vector<uint64_t> vers_asc(n);
    for (size_t i = 0; i < n; ++i) {
        vers_asc[i] = PackedVersion(key.packed_versions[n - 1 - i]).version();
    }

    std::unique_ptr<bool[]> keep(new bool[n]);
    dedupVersionGroup(vers_asc.data(), n, snapshot_versions, keep.get());

    std::vector<uint64_t> new_pvs;
    std::vector<uint32_t> new_ids;
    size_t removed = 0;
    for (size_t i = 0; i < n; ++i) {
        if (keep[n - 1 - i]) {
            new_pvs.push_back(key.packed_versions[i]);
            new_ids.push_back(key.ids[i]);
        } else {
            ++removed;
        }
    }
    key.packed_versions = std::move(new_pvs);
    key.ids = std::move(new_ids);
    return removed;
}

// Re-encode a bucket chain from a list of keys.
void ReadWriteDeltaHashTable::rewriteBucket(uint32_t bi,
                                             const std::vector<KeyEntry>& keys) {
    clearBucketChain(bi);
    for (const auto& key : keys) {
        for (size_t i = 0; i < key.packed_versions.size(); ++i) {
            addToChain(bi, key.suffix, key.packed_versions[i], key.ids[i],
                [this](Bucket& bucket) -> Bucket* {
                    return createExtension(bucket);
                });
        }
    }
    updateUsedBits(bi);
}

size_t ReadWriteDeltaHashTable::pruneStaleVersions(
        const std::vector<uint64_t>& snapshot_versions) {
    uint32_t num_buckets = 1u << config_.bucket_bits;
    size_t total_removed = 0;

    for (uint32_t bi = 0; bi < num_buckets; ++bi) {
        if (!multi_version_.test(bi)) continue;

        SpinlockGuard guard(bucket_locks_[bi]);

        auto t0 = now();
        auto all_keys = gatherBucketKeys(bi);
        trackTime(decode_count_, decode_total_ns_, t0);

        size_t removed = 0;
        bool still_multi = false;
        for (auto& key : all_keys) {
            if (key.packed_versions.size() > 1) {
                removed += dedupKeyVersions(key, snapshot_versions);
            }
            if (key.packed_versions.size() > 1) still_multi = true;
        }

        if (removed > 0) {
            rewriteBucket(bi, all_keys);
            total_removed += removed;
            size_.fetch_sub(removed, std::memory_order_relaxed);
        }

        if (still_multi) multi_version_.set(bi); else multi_version_.clear(bi);
    }

    return total_removed;
}

double ReadWriteDeltaHashTable::bucketUtilization() const {
    uint32_t num_buckets = 1u << config_.bucket_bits;
    size_t capacity_bits = static_cast<size_t>(num_buckets) * codec_.bucketDataBits();
    if (capacity_bits == 0) return 0.0;
    return static_cast<double>(used_bits_.sum()) / capacity_bits;
}

// --- Stats ---

size_t ReadWriteDeltaHashTable::size() const {
    return size_.load(std::memory_order_relaxed);
}

size_t ReadWriteDeltaHashTable::memoryUsage() const {
    return DeltaHashTable::memoryUsage()
         + (1u << config_.bucket_bits) * sizeof(Spinlock);
}

void ReadWriteDeltaHashTable::clear() {
    clearBuckets();
    size_.store(0, std::memory_order_relaxed);
}

// --- Binary snapshot support ---

uint32_t ReadWriteDeltaHashTable::extCount() const {
    return ext_arena_owned_.size();
}

const uint8_t* ReadWriteDeltaHashTable::extSlotData(uint32_t one_based) const {
    const Bucket* b = ext_arena_owned_.get(one_based);
    return b->data;
}

void ReadWriteDeltaHashTable::loadExtensions(const uint8_t* data, uint32_t count,
                                              uint32_t data_stride) {
    ext_arena_owned_.clear();
    for (uint32_t i = 0; i < count; ++i) {
        uint32_t idx = ext_arena_owned_.allocate();
        Bucket* b = ext_arena_owned_.get(idx);
        std::memcpy(b->data, data + static_cast<size_t>(i) * data_stride, data_stride);
    }
}

void ReadWriteDeltaHashTable::setSize(size_t n) {
    size_.store(n, std::memory_order_relaxed);
}

uint32_t ReadWriteDeltaHashTable::snapshotBucketChain(
    uint32_t bi, std::vector<uint8_t>& buf) const {
    SpinlockGuard guard(bucket_locks_[bi]);
    const Bucket* b = &buckets_[bi];
    if (isEmptyBucket(*b) && !nextBucket(*b)) return 0;

    uint32_t stride = bucketStride();
    buf.clear();
    uint32_t chain_len = 0;
    while (b) {
        buf.insert(buf.end(), b->data, b->data + stride);
        ++chain_len;
        b = nextBucket(*b);
    }
    return chain_len;
}

void ReadWriteDeltaHashTable::loadBucketChain(
    uint32_t bi, const uint8_t* data, uint8_t chain_len) {
    uint32_t stride = bucketStride();
    // Main bucket
    std::memcpy(buckets_[bi].data, data, stride);
    Bucket* prev = &buckets_[bi];
    for (uint8_t i = 1; i < chain_len; ++i) {
        uint32_t slot = ext_arena_owned_.allocate();
        Bucket* ext = ext_arena_owned_.get(slot);
        std::memcpy(ext->data, data + static_cast<size_t>(i) * stride, stride);
        setExtensionPtr(*prev, slot);
        prev = ext;
    }
    setExtensionPtr(*prev, 0);  // terminate chain
}

}  // namespace internal
}  // namespace kvlite
