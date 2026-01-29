#include "internal/l2_delta_hash_table.h"

#include <algorithm>
#include <functional>

namespace kvlite {
namespace internal {

L2DeltaHashTable::L2DeltaHashTable() : L2DeltaHashTable(Config{}) {}

L2DeltaHashTable::L2DeltaHashTable(const Config& config)
    : Base(config) {}

L2DeltaHashTable::~L2DeltaHashTable() = default;

// --- Find ---

bool L2DeltaHashTable::findAll(const std::string& key,
                                std::vector<uint32_t>& offsets,
                                std::vector<uint32_t>& versions) const {
    uint64_t h = hashKey(key);
    uint32_t bi = bucketIndex(h);
    uint32_t li = lslotIndex(h);
    uint64_t fp = fingerprint(h);

    offsets.clear();
    versions.clear();
    findInChainImpl(bi, li, fp, [&](const TrieEntry& entry) {
        offsets.insert(offsets.end(), entry.offsets.begin(), entry.offsets.end());
        versions.insert(versions.end(), entry.versions.begin(),
                        entry.versions.end());
    });
    return !offsets.empty();
}

bool L2DeltaHashTable::findFirst(const std::string& key,
                                  uint32_t& offset, uint32_t& version) const {
    uint64_t h = hashKey(key);
    uint32_t bi = bucketIndex(h);
    uint32_t li = lslotIndex(h);
    uint64_t fp = fingerprint(h);

    const Bucket* bucket = &buckets_[bi];
    while (bucket) {
        size_t bit_off = lslot_codec_.bitOffset(bucket->data.data(), li);
        LSlotContents contents = lslot_codec_.decode(bucket->data.data(), bit_off);

        for (const auto& entry : contents.entries) {
            if (entry.fingerprint == fp && !entry.offsets.empty()) {
                offset = entry.offsets[0];
                version = entry.versions[0];
                return true;
            }
        }

        bucket = nextBucket(*bucket);
    }
    return false;
}

bool L2DeltaHashTable::contains(const std::string& key) const {
    uint32_t off, ver;
    return findFirst(key, off, ver);
}

// --- Add ---

void L2DeltaHashTable::addEntry(const std::string& key,
                                 uint32_t offset, uint32_t version) {
    uint64_t h = hashKey(key);
    uint32_t bi = bucketIndex(h);
    uint32_t li = lslotIndex(h);
    uint64_t fp = fingerprint(h);

    addToChainImpl(bi, li,
        [fp, offset, version](std::vector<TrieEntry>& entries) {
            for (auto& entry : entries) {
                if (entry.fingerprint == fp) {
                    auto it = std::lower_bound(entry.offsets.begin(),
                                               entry.offsets.end(),
                                               offset, std::greater<uint32_t>());
                    size_t pos = it - entry.offsets.begin();
                    entry.offsets.insert(it, offset);
                    entry.versions.insert(entry.versions.begin() + pos, version);
                    return;
                }
            }
            TrieEntry new_entry;
            new_entry.fingerprint = fp;
            new_entry.offsets.push_back(offset);
            new_entry.versions.push_back(version);
            entries.push_back(std::move(new_entry));
            std::sort(entries.begin(), entries.end(),
                      [](const TrieEntry& a, const TrieEntry& b) {
                          return a.fingerprint < b.fingerprint;
                      });
        },
        [fp, offset, version](std::vector<TrieEntry>& entries) {
            for (auto it = entries.begin(); it != entries.end(); ++it) {
                if (it->fingerprint == fp) {
                    for (size_t i = 0; i < it->offsets.size(); ++i) {
                        if (it->offsets[i] == offset &&
                            it->versions[i] == version) {
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
            return createExtension(bucket);
        }
    );
    ++size_;
}

void L2DeltaHashTable::addEntryByHash(uint64_t hash,
                                       uint32_t offset, uint32_t version) {
    uint32_t bi = bucketIndex(hash);
    uint32_t li = lslotIndex(hash);
    uint64_t fp = fingerprint(hash);

    addToChainImpl(bi, li,
        [fp, offset, version](std::vector<TrieEntry>& entries) {
            for (auto& entry : entries) {
                if (entry.fingerprint == fp) {
                    auto it = std::lower_bound(entry.offsets.begin(),
                                               entry.offsets.end(),
                                               offset, std::greater<uint32_t>());
                    size_t pos = it - entry.offsets.begin();
                    entry.offsets.insert(it, offset);
                    entry.versions.insert(entry.versions.begin() + pos, version);
                    return;
                }
            }
            TrieEntry new_entry;
            new_entry.fingerprint = fp;
            new_entry.offsets.push_back(offset);
            new_entry.versions.push_back(version);
            entries.push_back(std::move(new_entry));
            std::sort(entries.begin(), entries.end(),
                      [](const TrieEntry& a, const TrieEntry& b) {
                          return a.fingerprint < b.fingerprint;
                      });
        },
        [fp, offset, version](std::vector<TrieEntry>& entries) {
            for (auto it = entries.begin(); it != entries.end(); ++it) {
                if (it->fingerprint == fp) {
                    for (size_t i = 0; i < it->offsets.size(); ++i) {
                        if (it->offsets[i] == offset &&
                            it->versions[i] == version) {
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
            return createExtension(bucket);
        }
    );
    ++size_;
}

// --- Iteration ---

void L2DeltaHashTable::forEach(
    const std::function<void(uint64_t hash, uint32_t offset,
                             uint32_t version)>& fn) const {
    forEachEntryImpl([&fn](uint64_t hash, const TrieEntry& entry) {
        for (size_t i = 0; i < entry.offsets.size(); ++i) {
            fn(hash, entry.offsets[i], entry.versions[i]);
        }
    });
}

void L2DeltaHashTable::forEachGroup(
    const std::function<void(uint64_t hash,
                             const std::vector<uint32_t>& offsets,
                             const std::vector<uint32_t>& versions)>& fn) const {
    uint32_t num_buckets = 1u << config_.bucket_bits;
    uint32_t n_lslots = numLSlots();

    for (uint32_t bi = 0; bi < num_buckets; ++bi) {
        struct Group {
            uint32_t lslot;
            uint64_t fp;
            std::vector<uint32_t> offsets;
            std::vector<uint32_t> versions;
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
                            g.offsets.insert(g.offsets.end(),
                                             entry.offsets.begin(),
                                             entry.offsets.end());
                            g.versions.insert(g.versions.end(),
                                              entry.versions.begin(),
                                              entry.versions.end());
                            merged = true;
                            break;
                        }
                    }
                    if (!merged) {
                        groups.push_back({s, entry.fingerprint,
                                          entry.offsets, entry.versions});
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
            fn(hash, g.offsets, g.versions);
        }
    }
}

// --- Stats ---

size_t L2DeltaHashTable::size() const {
    return size_;
}

size_t L2DeltaHashTable::memoryUsage() const {
    return bucketMemoryUsage();
}

void L2DeltaHashTable::clear() {
    clearBuckets();
    size_ = 0;
}

}  // namespace internal
}  // namespace kvlite
