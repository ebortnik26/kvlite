#include "internal/global_delta_hash_table.h"

#include <algorithm>
#include <functional>

namespace kvlite {
namespace internal {

GlobalDeltaHashTable::GlobalDeltaHashTable() : GlobalDeltaHashTable(Config{}) {}

GlobalDeltaHashTable::GlobalDeltaHashTable(const Config& config)
    : Base(config) {
    bucket_locks_ = std::make_unique<BucketLock[]>(1u << config.bucket_bits);
}

GlobalDeltaHashTable::~GlobalDeltaHashTable() = default;

// --- Find ---

bool GlobalDeltaHashTable::findAll(const std::string& key,
                                    std::vector<uint32_t>& packed_versions,
                                    std::vector<uint32_t>& segment_ids) const {
    uint64_t h = hashKey(key);
    uint32_t bi = bucketIndex(h);
    uint32_t li = lslotIndex(h);
    uint64_t fp = fingerprint(h);

    BucketLockGuard guard(const_cast<GlobalDeltaHashTable*>(this)->bucket_locks_[bi]);
    packed_versions.clear();
    segment_ids.clear();
    findInChainImpl(bi, li, fp, [&](const TrieEntry& entry) {
        packed_versions.insert(packed_versions.end(),
                               entry.offsets.begin(),
                               entry.offsets.end());
        segment_ids.insert(segment_ids.end(),
                           entry.versions.begin(),
                           entry.versions.end());
    });
    return !packed_versions.empty();
}

bool GlobalDeltaHashTable::findFirst(const std::string& key,
                                      uint32_t& packed_version,
                                      uint32_t& segment_id) const {
    uint64_t h = hashKey(key);
    uint32_t bi = bucketIndex(h);
    uint32_t li = lslotIndex(h);
    uint64_t fp = fingerprint(h);

    BucketLockGuard guard(const_cast<GlobalDeltaHashTable*>(this)->bucket_locks_[bi]);

    const Bucket* bucket = &buckets_[bi];
    while (bucket) {
        LSlotContents contents = decodeLSlot(*bucket, li);

        for (const auto& entry : contents.entries) {
            if (entry.fingerprint == fp && !entry.offsets.empty()) {
                // Return the entry with the highest packed_version.
                auto max_it = std::max_element(entry.offsets.begin(),
                                               entry.offsets.end());
                size_t idx = max_it - entry.offsets.begin();
                packed_version = entry.offsets[idx];
                segment_id = entry.versions[idx];
                return true;
            }
        }

        bucket = nextBucket(*bucket);
    }
    return false;
}

bool GlobalDeltaHashTable::contains(const std::string& key) const {
    uint32_t pv, seg;
    return findFirst(key, pv, seg);
}

// --- Add ---

void GlobalDeltaHashTable::addEntry(const std::string& key,
                                     uint32_t packed_version,
                                     uint32_t segment_id) {
    uint64_t h = hashKey(key);
    addImpl(bucketIndex(h), lslotIndex(h), fingerprint(h),
            packed_version, segment_id);
}

void GlobalDeltaHashTable::addEntryByHash(uint64_t hash,
                                           uint32_t packed_version,
                                           uint32_t segment_id) {
    addImpl(bucketIndex(hash), lslotIndex(hash), fingerprint(hash),
            packed_version, segment_id);
}

void GlobalDeltaHashTable::addImpl(uint32_t bi, uint32_t li, uint64_t fp,
                                    uint32_t packed_version,
                                    uint32_t segment_id) {
    BucketLockGuard guard(bucket_locks_[bi]);

    addToChainImpl(bi, li,
        [fp, packed_version, segment_id](std::vector<TrieEntry>& entries) {
            for (auto& entry : entries) {
                if (entry.fingerprint == fp) {
                    auto it = std::lower_bound(entry.offsets.begin(),
                                               entry.offsets.end(),
                                               packed_version,
                                               std::greater<uint32_t>());
                    size_t pos = it - entry.offsets.begin();
                    entry.offsets.insert(it, packed_version);
                    entry.versions.insert(entry.versions.begin() + pos,
                                             segment_id);
                    return;
                }
            }
            TrieEntry new_entry;
            new_entry.fingerprint = fp;
            new_entry.offsets.push_back(packed_version);
            new_entry.versions.push_back(segment_id);
            entries.push_back(std::move(new_entry));
            std::sort(entries.begin(), entries.end(),
                      [](const TrieEntry& a, const TrieEntry& b) {
                          return a.fingerprint < b.fingerprint;
                      });
        },
        [fp, packed_version, segment_id](std::vector<TrieEntry>& entries) {
            for (auto it = entries.begin(); it != entries.end(); ++it) {
                if (it->fingerprint == fp) {
                    for (size_t i = 0; i < it->offsets.size(); ++i) {
                        if (it->offsets[i] == packed_version &&
                            it->versions[i] == segment_id) {
                            it->offsets.erase(it->offsets.begin() + i);
                            it->versions.erase(it->versions.begin() + i);
                            break;
                        }
                    }
                    if (it->offsets.empty()) {
                        entries.erase(it);
                    }
                    break;
                }
            }
        },
        [this](Bucket& bucket) -> Bucket* {
            std::lock_guard<std::mutex> g(ext_mutex_);
            return createExtension(bucket);
        }
    );
    size_.fetch_add(1, std::memory_order_relaxed);
}

// --- Iteration ---

void GlobalDeltaHashTable::forEach(
    const std::function<void(uint64_t hash, uint32_t packed_version,
                             uint32_t segment_id)>& fn) const {
    forEachEntryImpl([&fn](uint64_t hash, const TrieEntry& entry) {
        for (size_t i = 0; i < entry.offsets.size(); ++i) {
            fn(hash, entry.offsets[i], entry.versions[i]);
        }
    });
}

void GlobalDeltaHashTable::forEachGroup(
    const std::function<void(uint64_t hash,
                             const std::vector<uint32_t>& packed_versions,
                             const std::vector<uint32_t>& segment_ids)>& fn) const {
    uint32_t num_buckets = 1u << config_.bucket_bits;
    uint32_t n_lslots = numLSlots();

    for (uint32_t bi = 0; bi < num_buckets; ++bi) {
        struct Group {
            uint32_t lslot;
            uint64_t fp;
            std::vector<uint32_t> packed_versions;
            std::vector<uint32_t> segment_ids;
        };
        std::vector<Group> groups;

        const Bucket* b = &buckets_[bi];
        while (b) {
            size_t offset = 0;
            for (uint32_t s = 0; s < n_lslots; ++s) {
                LSlotContents contents =
                    lslot_codec_.decode(b->data, offset, &offset);
                for (const auto& entry : contents.entries) {
                    bool merged = false;
                    for (auto& g : groups) {
                        if (g.lslot == s && g.fp == entry.fingerprint) {
                            g.packed_versions.insert(g.packed_versions.end(),
                                                     entry.offsets.begin(),
                                                     entry.offsets.end());
                            g.segment_ids.insert(g.segment_ids.end(),
                                                 entry.versions.begin(),
                                                 entry.versions.end());
                            merged = true;
                            break;
                        }
                    }
                    if (!merged) {
                        groups.push_back({s, entry.fingerprint,
                                          entry.offsets,
                                          entry.versions});
                    }
                }
            }
            b = nextBucket(*b);
        }

        for (const auto& g : groups) {
            uint64_t hash =
                (static_cast<uint64_t>(bi) << (64 - config_.bucket_bits)) |
                (static_cast<uint64_t>(g.lslot) << fingerprint_bits_) |
                g.fp;
            fn(hash, g.packed_versions, g.segment_ids);
        }
    }
}

// --- Stats ---

size_t GlobalDeltaHashTable::size() const {
    return size_.load(std::memory_order_relaxed);
}

size_t GlobalDeltaHashTable::memoryUsage() const {
    return bucketMemoryUsage()
         + (1u << config_.bucket_bits) * sizeof(BucketLock);
}

void GlobalDeltaHashTable::clear() {
    clearBuckets();
    size_.store(0, std::memory_order_relaxed);
}

}  // namespace internal
}  // namespace kvlite
