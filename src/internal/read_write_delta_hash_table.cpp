#include "internal/read_write_delta_hash_table.h"

#include <algorithm>
#include <cstring>
#include <map>

#include "internal/version_dedup.h"

namespace kvlite {
namespace internal {

ReadWriteDeltaHashTable::ReadWriteDeltaHashTable()
    : ReadWriteDeltaHashTable(Config{}) {}

ReadWriteDeltaHashTable::ReadWriteDeltaHashTable(const Config& config)
    : DeltaHashTable(config),
      ext_arena_owned_(sizeof(Bucket) + bucketStride(), /*concurrent=*/true),
      duplicates_(1u << config.bucket_bits) {
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
    bool actually_removed = false;
    bool group_empty = removeFromChain(bi, suffix, packed_version, id,
                                        actually_removed);
    if (!actually_removed) return false;
    size_.fetch_sub(1, std::memory_order_relaxed);
    return group_empty;
}

bool ReadWriteDeltaHashTable::updateEntryId(uint64_t hash,
                                              uint64_t packed_version,
                                              uint32_t old_id, uint32_t new_id) {
    uint32_t bi = bucketIndex(hash);
    uint64_t suffix = suffixFromHash(hash);

    SpinlockGuard guard(bucket_locks_[bi]);
    return updateIdInChain(bi, suffix, packed_version, old_id, new_id,
        [this](Bucket& bucket) -> Bucket* {
            return createExtension(bucket);
        });
}

bool ReadWriteDeltaHashTable::addImpl(uint32_t bi, uint64_t suffix,
                                       uint64_t packed_version, uint32_t id) {
    SpinlockGuard guard(bucket_locks_[bi]);

    bool is_new = addToChain(bi, suffix, packed_version, id,
        [this](Bucket& bucket) -> Bucket* {
            return createExtension(bucket);
        });
    size_.fetch_add(1, std::memory_order_relaxed);
    if (!is_new) duplicates_.set(bi);
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
        bool any_dup = false;
        while (j < count && bucketIndex(entries[j].hash) == bi) {
            bool is_new = addToChain(bi, suffixFromHash(entries[j].hash),
                entries[j].packed_version, id,
                [this](Bucket& bucket) -> Bucket* {
                    return createExtension(bucket);
                });
            if (is_new) ++new_keys;
            else any_dup = true;
            ++j;
        }
        size_.fetch_add(j - i, std::memory_order_relaxed);
        if (any_dup) duplicates_.set(bi);
        i = j;
    }
    return new_keys;
}

ReadWriteDeltaHashTable::BatchResult
ReadWriteDeltaHashTable::addEntriesBatchPrune(
        const HashVersionPair* entries, size_t count, uint32_t id,
        const std::vector<uint64_t>& snapshot_versions) {
    size_t new_keys = 0;
    size_t total_pruned = 0;
    size_t i = 0;
    while (i < count) {
        uint32_t bi = bucketIndex(entries[i].hash);
        SpinlockGuard guard(bucket_locks_[bi]);

        size_t j = i;
        size_t bucket_pruned = 0;
        bool any_dup = false;
        while (j < count && bucketIndex(entries[j].hash) == bi) {
            bool is_new = addToChain(bi, suffixFromHash(entries[j].hash),
                entries[j].packed_version, id,
                [this](Bucket& bucket) -> Bucket* {
                    return createExtension(bucket);
                },
                &snapshot_versions, &bucket_pruned);
            if (is_new) ++new_keys;
            else any_dup = true;
            ++j;
        }
        size_.fetch_add(j - i - bucket_pruned, std::memory_order_relaxed);
        total_pruned += bucket_pruned;
        // Inline dedup may leave some multi-version keys (snapshot-visible).
        // Mark the bucket so the background daemon revisits if snapshots
        // change later.
        if (any_dup) duplicates_.set(bi);
        i = j;
    }
    return {new_keys, total_pruned};
}

size_t ReadWriteDeltaHashTable::pruneStaleVersions(
        const std::vector<uint64_t>& snapshots) {
    uint32_t num_buckets = 1u << config_.bucket_bits;
    size_t total_removed = 0;

    for (uint32_t bi = 0; bi < num_buckets; ++bi) {
        if (!duplicates_.test(bi)) continue;

        SpinlockGuard guard(bucket_locks_[bi]);
        total_removed += prunePrimaryBucketChain(bi, snapshots);
    }

    if (total_removed > 0) {
        size_.fetch_sub(total_removed, std::memory_order_relaxed);
    }
    return total_removed;
}

// Merge all chain buckets into a single flat BucketContents, with each
// suffix appearing exactly once (versions merged desc across the chain).
static BucketContents mergeChain(const std::vector<BucketContents>& chain) {
    std::map<uint64_t, KeyEntry> merged;  // keyed by suffix, sorted asc
    for (const auto& c : chain) {
        for (const auto& key : c.keys) {
            auto& dst = merged[key.suffix];
            if (dst.packed_versions.empty()) {
                dst.suffix = key.suffix;
                dst.packed_versions = key.packed_versions;
                dst.ids = key.ids;
            } else {
                // Merge two desc-sorted lists.
                std::vector<uint64_t> out_pvs;
                std::vector<uint32_t> out_ids;
                size_t a = 0, b = 0;
                const auto& ap = dst.packed_versions;
                const auto& ai = dst.ids;
                const auto& bp = key.packed_versions;
                const auto& bi_ = key.ids;
                out_pvs.reserve(ap.size() + bp.size());
                out_ids.reserve(ai.size() + bi_.size());
                while (a < ap.size() && b < bp.size()) {
                    if (ap[a] >= bp[b]) { out_pvs.push_back(ap[a]); out_ids.push_back(ai[a]); ++a; }
                    else                { out_pvs.push_back(bp[b]); out_ids.push_back(bi_[b]); ++b; }
                }
                while (a < ap.size()) { out_pvs.push_back(ap[a]); out_ids.push_back(ai[a]); ++a; }
                while (b < bp.size()) { out_pvs.push_back(bp[b]); out_ids.push_back(bi_[b]); ++b; }
                dst.packed_versions = std::move(out_pvs);
                dst.ids = std::move(out_ids);
            }
        }
    }
    BucketContents flat;
    flat.keys.reserve(merged.size());
    for (auto& [suffix, key] : merged) flat.keys.push_back(std::move(key));
    return flat;
}

// Prune stale versions across one primary bucket's full chain.
// Decode chain → merge → prune → re-encode into primary + extensions.
// Must hold bucket_locks_[bi].
size_t ReadWriteDeltaHashTable::prunePrimaryBucketChain(
        uint32_t bi, const std::vector<uint64_t>& snapshots) {
    // Decode the chain into a list of buckets.
    std::vector<BucketContents> chain;
    std::vector<Bucket*> chain_buckets;
    for (Bucket* b = &buckets_[bi]; b; b = nextBucketMut(*b)) {
        chain_buckets.push_back(b);
        chain.push_back(codec_.decodeBucket(*b));
    }

    // Merge into a single flat contents.
    BucketContents flat = mergeChain(chain);

    // Prune each key against the snapshot set.
    size_t removed = 0;
    bool still_multi = false;
    for (auto& key : flat.keys) {
        if (key.packed_versions.size() > 1) {
            removed += pruneKeyVersionsDesc(key.packed_versions, key.ids, snapshots);
            if (key.packed_versions.size() > 1) still_multi = true;
        }
    }

    if (removed == 0) {
        if (!still_multi) duplicates_.clear(bi);
        return 0;
    }

    // Re-encode flat into the chain: fill primary, overflow to
    // existing extensions.  Chain length is sufficient since we only
    // removed entries.
    size_t chain_idx = 0;
    BucketContents cur;
    for (auto& key : flat.keys) {
        cur.keys.push_back(std::move(key));
        if (codec_.contentsBitsNeeded(cur) > codec_.bucketDataBits()) {
            auto overflow = std::move(cur.keys.back());
            cur.keys.pop_back();
            codec_.encodeBucket(*chain_buckets[chain_idx++], cur);
            cur.keys.clear();
            cur.keys.push_back(std::move(overflow));
        }
    }
    codec_.encodeBucket(*chain_buckets[chain_idx++], cur);
    // Clear any remaining (now-unused) extension buckets.
    BucketContents empty;
    while (chain_idx < chain_buckets.size()) {
        codec_.encodeBucket(*chain_buckets[chain_idx++], empty);
    }

    if (!still_multi) duplicates_.clear(bi);
    return removed;
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
    duplicates_.clearAll();
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
