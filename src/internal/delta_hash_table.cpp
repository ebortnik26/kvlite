#include "internal/delta_hash_table.h"

#include <algorithm>
#include <functional>

namespace kvlite {
namespace internal {

DeltaHashTable::DeltaHashTable() : DeltaHashTable(Config{}) {}

DeltaHashTable::DeltaHashTable(const Config& config)
    : Base(config) {
    bucket_locks_ = std::make_unique<BucketLock[]>(1u << config.bucket_bits);
}

DeltaHashTable::~DeltaHashTable() = default;

// --- Find ---

bool DeltaHashTable::findAll(const std::string& key,
                              std::vector<uint32_t>& out) const {
    uint64_t h = hashKey(key);
    uint32_t bi = bucketIndex(h);
    uint32_t li = lslotIndex(h);
    uint64_t fp = fingerprint(h);

    BucketLockGuard guard(const_cast<DeltaHashTable*>(this)->bucket_locks_[bi]);
    out.clear();
    findInChainImpl(bi, li, fp, [&out](const TrieEntry& entry) {
        out.insert(out.end(), entry.values.begin(), entry.values.end());
    });
    return !out.empty();
}

bool DeltaHashTable::findFirst(const std::string& key, uint32_t& value) const {
    uint64_t h = hashKey(key);
    uint32_t bi = bucketIndex(h);
    uint32_t li = lslotIndex(h);
    uint64_t fp = fingerprint(h);

    BucketLockGuard guard(const_cast<DeltaHashTable*>(this)->bucket_locks_[bi]);

    const Bucket* bucket = &buckets_[bi];
    while (bucket) {
        LSlotContents contents = decodeLSlot(*bucket, li);

        for (const auto& entry : contents.entries) {
            if (entry.fingerprint == fp && !entry.values.empty()) {
                value = entry.values[0];
                return true;
            }
        }

        bucket = nextBucket(*bucket);
    }
    return false;
}

bool DeltaHashTable::contains(const std::string& key) const {
    uint32_t dummy;
    return findFirst(key, dummy);
}

// --- Add ---

void DeltaHashTable::addEntry(const std::string& key, uint32_t value) {
    uint64_t h = hashKey(key);
    addImpl(bucketIndex(h), lslotIndex(h), fingerprint(h), value);
}

void DeltaHashTable::addEntryByHash(uint64_t hash, uint32_t value) {
    addImpl(bucketIndex(hash), lslotIndex(hash), fingerprint(hash), value);
}

void DeltaHashTable::addImpl(uint32_t bi, uint32_t li, uint64_t fp,
                              uint32_t value) {
    BucketLockGuard guard(bucket_locks_[bi]);

    // Check for duplicate across chain.
    bool dup = false;
    findInChainImpl(bi, li, fp, [&](const TrieEntry& entry) {
        for (uint32_t v : entry.values) {
            if (v == value) dup = true;
        }
    });
    if (dup) return;

    addToChainImpl(bi, li,
        [fp, value](std::vector<TrieEntry>& entries) {
            bool found_fp = false;
            for (auto& entry : entries) {
                if (entry.fingerprint == fp) {
                    auto it = std::lower_bound(entry.values.begin(),
                                               entry.values.end(),
                                               value, std::greater<uint32_t>());
                    entry.values.insert(it, value);
                    found_fp = true;
                    break;
                }
            }
            if (!found_fp) {
                TrieEntry new_entry;
                new_entry.fingerprint = fp;
                new_entry.values.push_back(value);
                entries.push_back(std::move(new_entry));
                std::sort(entries.begin(), entries.end(),
                          [](const TrieEntry& a, const TrieEntry& b) {
                              return a.fingerprint < b.fingerprint;
                          });
            }
        },
        [fp, value](std::vector<TrieEntry>& entries) {
            for (auto it = entries.begin(); it != entries.end(); ++it) {
                if (it->fingerprint == fp) {
                    auto vit = std::find(it->values.begin(), it->values.end(),
                                         value);
                    if (vit != it->values.end()) {
                        it->values.erase(vit);
                    }
                    if (it->values.empty()) {
                        entries.erase(it);
                    }
                    break;
                }
            }
        },
        [this](Bucket& bucket) -> Bucket* {
            std::lock_guard<std::mutex> ext_guard(ext_mutex_);
            return createExtension(bucket);
        }
    );
    size_.fetch_add(1, std::memory_order_relaxed);
}

// --- Remove ---

bool DeltaHashTable::removeEntry(const std::string& key, uint32_t value) {
    uint64_t h = hashKey(key);
    uint32_t bi = bucketIndex(h);
    uint32_t li = lslotIndex(h);
    uint64_t fp = fingerprint(h);

    BucketLockGuard guard(bucket_locks_[bi]);

    bool removed = modifyInChainImpl(bi, li,
        [fp, value](std::vector<TrieEntry>& entries) -> bool {
            for (auto it = entries.begin(); it != entries.end(); ++it) {
                if (it->fingerprint == fp) {
                    auto vit = std::find(it->values.begin(), it->values.end(),
                                         value);
                    if (vit != it->values.end()) {
                        it->values.erase(vit);
                        if (it->values.empty()) {
                            entries.erase(it);
                        }
                        return true;
                    }
                }
            }
            return false;
        });

    if (removed) {
        size_.fetch_sub(1, std::memory_order_relaxed);
    }
    return removed;
}

size_t DeltaHashTable::removeAll(const std::string& key) {
    uint64_t h = hashKey(key);
    uint32_t bi = bucketIndex(h);
    uint32_t li = lslotIndex(h);
    uint64_t fp = fingerprint(h);

    BucketLockGuard guard(bucket_locks_[bi]);

    size_t removed = removeAllFromChain(bi, li, fp);
    if (removed > 0) {
        size_.fetch_sub(removed, std::memory_order_relaxed);
    }
    return removed;
}

size_t DeltaHashTable::removeAllFromChain(uint32_t bucket_idx,
                                           uint32_t lslot_idx,
                                           uint64_t fp) {
    Bucket* bucket = &buckets_[bucket_idx];
    size_t total_removed = 0;

    while (bucket) {
        auto all_slots = decodeAllLSlots(*bucket);
        auto& entries = all_slots[lslot_idx].entries;
        size_t removed = 0;

        for (auto it = entries.begin(); it != entries.end(); ++it) {
            if (it->fingerprint == fp) {
                removed = it->values.size();
                entries.erase(it);
                break;
            }
        }

        if (removed > 0) {
            total_removed += removed;
            reencodeAllLSlots(*bucket, all_slots);
        }

        bucket = nextBucketMut(*bucket);
    }

    return total_removed;
}

// --- Iteration ---

void DeltaHashTable::forEach(
    const std::function<void(uint64_t hash, uint32_t value)>& fn) const {
    forEachEntryImpl([&fn](uint64_t hash, const TrieEntry& entry) {
        for (uint32_t v : entry.values) {
            fn(hash, v);
        }
    });
}

void DeltaHashTable::forEachGroup(
    const std::function<void(uint64_t hash,
                             const std::vector<uint32_t>&)>& fn) const {
    uint32_t num_buckets = 1u << config_.bucket_bits;
    uint32_t n_lslots = numLSlots();

    for (uint32_t bi = 0; bi < num_buckets; ++bi) {
        struct Group {
            uint32_t lslot;
            uint64_t fp;
            std::vector<uint32_t> values;
        };
        std::vector<Group> groups;

        const Bucket* b = &buckets_[bi];
        while (b) {
            size_t offset = 0;
            for (uint32_t s = 0; s < n_lslots; ++s) {
                LSlotContents contents = lslot_codec_.decode(
                    b->data.data(), offset, &offset);
                for (const auto& entry : contents.entries) {
                    bool merged = false;
                    for (auto& g : groups) {
                        if (g.lslot == s && g.fp == entry.fingerprint) {
                            g.values.insert(g.values.end(),
                                            entry.values.begin(),
                                            entry.values.end());
                            merged = true;
                            break;
                        }
                    }
                    if (!merged) {
                        groups.push_back({s, entry.fingerprint, entry.values});
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
            fn(hash, g.values);
        }
    }
}

// --- Stats ---

size_t DeltaHashTable::size() const {
    return size_.load(std::memory_order_relaxed);
}

size_t DeltaHashTable::memoryUsage() const {
    return bucketMemoryUsage()
         + (1u << config_.bucket_bits) * sizeof(BucketLock);
}

void DeltaHashTable::clear() {
    clearBuckets();
    size_.store(0, std::memory_order_relaxed);
}

}  // namespace internal
}  // namespace kvlite
