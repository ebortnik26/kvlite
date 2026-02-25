#include "internal/delta_hash_table.h"

#include <algorithm>
#include <cstring>
#include <unordered_map>

#include "internal/bit_stream.h"

namespace kvlite {
namespace internal {

// --- Construction / Move ---

DeltaHashTable::DeltaHashTable(const Config& config)
    : config_(config),
      fingerprint_bits_(64 - config.bucket_bits - config.lslot_bits),
      lslot_codec_(fingerprint_bits_) {
    uint32_t num_buckets = 1u << config_.bucket_bits;
    uint32_t stride = bucketStride();
    arena_ = std::make_unique<uint8_t[]>(
        static_cast<size_t>(num_buckets) * stride);
    std::memset(arena_.get(), 0,
                static_cast<size_t>(num_buckets) * stride);
    buckets_.resize(num_buckets);
    for (uint32_t i = 0; i < num_buckets; ++i) {
        buckets_[i].data = arena_.get() + static_cast<size_t>(i) * stride;
    }
    clearBuckets();
}

DeltaHashTable::~DeltaHashTable() = default;
DeltaHashTable::DeltaHashTable(DeltaHashTable&&) noexcept = default;
DeltaHashTable& DeltaHashTable::operator=(DeltaHashTable&&) noexcept = default;

// --- Hash decomposition ---

uint64_t DeltaHashTable::hashKey(std::string_view key) const {
    return dhtHashBytes(key.data(), key.size());
}

uint32_t DeltaHashTable::bucketIndex(uint64_t hash) const {
    return static_cast<uint32_t>(hash >> (64 - config_.bucket_bits));
}

uint32_t DeltaHashTable::lslotIndex(uint64_t hash) const {
    return static_cast<uint32_t>(
        (hash >> (64 - config_.bucket_bits - config_.lslot_bits))
        & ((1u << config_.lslot_bits) - 1));
}

uint64_t DeltaHashTable::fingerprint(uint64_t hash) const {
    return hash & ((1ULL << fingerprint_bits_) - 1);
}

// --- Bucket data ---

uint32_t DeltaHashTable::bucketStride() const {
    return config_.bucket_bytes + kBucketPadding;
}

uint64_t DeltaHashTable::getExtensionPtr(const Bucket& bucket) const {
    uint64_t ptr = 0;
    std::memcpy(&ptr, bucket.data + config_.bucket_bytes - 8, 8);
    return ptr;
}

void DeltaHashTable::setExtensionPtr(Bucket& bucket, uint64_t ptr) const {
    std::memcpy(bucket.data + config_.bucket_bytes - 8, &ptr, 8);
}

size_t DeltaHashTable::bucketDataBits() const {
    return (config_.bucket_bytes - 8) * 8;
}

uint32_t DeltaHashTable::numLSlots() const {
    return 1u << config_.lslot_bits;
}

// --- Decode / Encode ---

DeltaHashTable::LSlotContents DeltaHashTable::decodeLSlot(
    const Bucket& bucket, uint32_t lslot_idx) const {
    size_t bit_off = lslot_codec_.bitOffset(bucket.data, lslot_idx);
    return lslot_codec_.decode(bucket.data, bit_off);
}

std::vector<DeltaHashTable::LSlotContents> DeltaHashTable::decodeAllLSlots(
    const Bucket& bucket) const {
    uint32_t n = numLSlots();
    std::vector<LSlotContents> slots(n);
    size_t offset = 0;
    for (uint32_t s = 0; s < n; ++s) {
        slots[s] = lslot_codec_.decode(bucket.data, offset, &offset);
    }
    return slots;
}

void DeltaHashTable::reencodeAllLSlots(
    Bucket& bucket, const std::vector<LSlotContents>& all_slots) {
    size_t data_bytes = config_.bucket_bytes - 8;
    uint64_t ext_ptr = getExtensionPtr(bucket);
    std::memset(bucket.data, 0, data_bytes);
    setExtensionPtr(bucket, ext_ptr);

    size_t write_offset = 0;
    for (uint32_t s = 0; s < all_slots.size(); ++s) {
        write_offset = lslot_codec_.encode(bucket.data, write_offset,
                                           all_slots[s]);
    }
}

size_t DeltaHashTable::totalBitsNeeded(
    const std::vector<LSlotContents>& all_slots) const {
    size_t bits = 0;
    for (const auto& slot : all_slots) {
        bits += LSlotCodec::bitsNeeded(slot, fingerprint_bits_);
    }
    return bits;
}

// --- Extension chain ---

const DeltaHashTable::Bucket* DeltaHashTable::nextBucket(
    const Bucket& bucket) const {
    uint64_t ext = getExtensionPtr(bucket);
    return ext ? extensions_[ext - 1].get() : nullptr;
}

DeltaHashTable::Bucket* DeltaHashTable::nextBucketMut(Bucket& bucket) {
    uint64_t ext = getExtensionPtr(bucket);
    return ext ? extensions_[ext - 1].get() : nullptr;
}

DeltaHashTable::Bucket* DeltaHashTable::createExtension(Bucket& bucket) {
    uint32_t stride = bucketStride();
    auto storage = std::make_unique<uint8_t[]>(stride);
    std::memset(storage.get(), 0, stride);
    auto ext = std::make_unique<Bucket>();
    ext->data = storage.get();
    ext_storage_.push_back(std::move(storage));
    extensions_.push_back(std::move(ext));
    uint64_t ext_ptr = extensions_.size();  // 1-based
    setExtensionPtr(bucket, ext_ptr);

    Bucket* ext_bucket = extensions_[ext_ptr - 1].get();
    initBucket(*ext_bucket);
    return ext_bucket;
}

// --- addToChain ---

bool DeltaHashTable::addToChain(uint32_t bi, uint32_t li, uint64_t fp,
                                 uint64_t packed_version, uint32_t id,
                                 const std::function<Bucket*(Bucket&)>& createExtFn) {
    Bucket* bucket = &buckets_[bi];
    bool is_new = true;

    while (true) {
        auto all_slots = decodeAllLSlots(*bucket);

        // Check if fingerprint already exists in this bucket's lslot.
        if (is_new) {
            for (const auto& entry : all_slots[li].entries) {
                if (entry.fingerprint == fp) {
                    is_new = false;
                    break;
                }
            }
        }

        // Compute current bit cost of the target lslot before insertion.
        size_t old_slot_bits = LSlotCodec::bitsNeeded(all_slots[li],
                                                       fingerprint_bits_);

        // Build a candidate copy with the new entry inserted.
        LSlotContents candidate = all_slots[li];
        auto& entries = candidate.entries;
        bool found = false;
        for (auto& entry : entries) {
            if (entry.fingerprint == fp) {
                auto it = std::lower_bound(entry.packed_versions.begin(),
                                           entry.packed_versions.end(),
                                           packed_version,
                                           std::greater<uint64_t>());
                size_t pos = it - entry.packed_versions.begin();
                entry.packed_versions.insert(it, packed_version);
                entry.ids.insert(entry.ids.begin() + pos, id);
                found = true;
                break;
            }
        }
        if (!found) {
            TrieEntry new_entry;
            new_entry.fingerprint = fp;
            new_entry.packed_versions.push_back(packed_version);
            new_entry.ids.push_back(id);
            entries.push_back(std::move(new_entry));
            std::sort(entries.begin(), entries.end(),
                      [](const TrieEntry& a, const TrieEntry& b) {
                          return a.fingerprint < b.fingerprint;
                      });
        }

        size_t new_slot_bits = LSlotCodec::bitsNeeded(candidate,
                                                       fingerprint_bits_);
        size_t total_bits = totalBitsNeeded(all_slots) - old_slot_bits
                            + new_slot_bits;

        if (total_bits <= bucketDataBits()) {
            all_slots[li] = std::move(candidate);
            reencodeAllLSlots(*bucket, all_slots);
            return is_new;
        }

        // Doesn't fit — discard candidate, walk to extension bucket.
        Bucket* ext = nextBucketMut(*bucket);
        if (!ext) {
            ext = createExtFn(*bucket);
        }
        bucket = ext;
    }
}

// --- removeFromChain ---

bool DeltaHashTable::removeFromChain(uint32_t bi, uint32_t li, uint64_t fp,
                                      uint64_t packed_version, uint32_t id) {
    Bucket* bucket = &buckets_[bi];

    while (bucket) {
        auto all_slots = decodeAllLSlots(*bucket);
        auto& entries = all_slots[li].entries;
        bool modified = false;

        for (auto it = entries.begin(); it != entries.end(); ++it) {
            if (it->fingerprint != fp) continue;
            // Find and remove the (packed_version, id) pair.
            for (size_t j = 0; j < it->packed_versions.size(); ++j) {
                if (it->packed_versions[j] == packed_version && it->ids[j] == id) {
                    it->packed_versions.erase(it->packed_versions.begin() + j);
                    it->ids.erase(it->ids.begin() + j);
                    modified = true;
                    break;
                }
            }
            if (modified && it->packed_versions.empty()) {
                entries.erase(it);
            }
            break;
        }

        if (modified) {
            reencodeAllLSlots(*bucket, all_slots);
        }

        // Check whether this bucket's extension is now entirely empty.
        Bucket* ext = nextBucketMut(*bucket);
        if (ext) {
            auto ext_slots = decodeAllLSlots(*ext);
            bool ext_empty = true;
            for (const auto& slot : ext_slots) {
                if (!slot.entries.empty()) {
                    ext_empty = false;
                    break;
                }
            }
            if (ext_empty && !nextBucket(*ext)) {
                // Tail extension is empty — unlink it.
                setExtensionPtr(*bucket, 0);
            }
        }

        bucket = nextBucketMut(*bucket);
    }

    // Check if the fingerprint group is completely gone across all buckets.
    const Bucket* b = &buckets_[bi];
    while (b) {
        LSlotContents contents = decodeLSlot(*b, li);
        for (const auto& entry : contents.entries) {
            if (entry.fingerprint == fp) {
                return false;  // group still exists
            }
        }
        b = nextBucket(*b);
    }
    return true;
}

// --- updateIdInChain ---

bool DeltaHashTable::updateIdInChain(uint32_t bi, uint32_t li, uint64_t fp,
                                      uint64_t packed_version, uint32_t old_id,
                                      uint32_t new_id,
                                      const std::function<Bucket*(Bucket&)>& createExtFn) {
    Bucket* bucket = &buckets_[bi];

    while (bucket) {
        auto all_slots = decodeAllLSlots(*bucket);
        auto& entries = all_slots[li].entries;
        bool found = false;

        for (auto& entry : entries) {
            if (entry.fingerprint != fp) continue;
            for (size_t j = 0; j < entry.packed_versions.size(); ++j) {
                if (entry.packed_versions[j] == packed_version && entry.ids[j] == old_id) {
                    entry.ids[j] = new_id;
                    found = true;
                    break;
                }
            }
            break;
        }

        if (!found) {
            bucket = nextBucketMut(*bucket);
            continue;
        }

        // Re-encode. Check if it still fits.
        size_t total_bits = totalBitsNeeded(all_slots);
        if (total_bits <= bucketDataBits()) {
            reencodeAllLSlots(*bucket, all_slots);
            return true;
        }

        // Overflow: spill the target lslot entry to extension bucket.
        // Remove the modified entry from this bucket's lslot and reencode.
        LSlotContents spill;
        // Find the modified TrieEntry and move it to spill.
        auto& slot_entries = all_slots[li].entries;
        for (auto it = slot_entries.begin(); it != slot_entries.end(); ++it) {
            if (it->fingerprint == fp) {
                spill.entries.push_back(std::move(*it));
                slot_entries.erase(it);
                break;
            }
        }
        reencodeAllLSlots(*bucket, all_slots);

        // Write spilled entry to extension.
        Bucket* ext = nextBucketMut(*bucket);
        if (!ext) {
            ext = createExtFn(*bucket);
        }
        auto ext_slots = decodeAllLSlots(*ext);
        ext_slots[li].entries.insert(ext_slots[li].entries.end(),
                                     spill.entries.begin(), spill.entries.end());
        // Sort by fingerprint.
        std::sort(ext_slots[li].entries.begin(), ext_slots[li].entries.end(),
                  [](const TrieEntry& a, const TrieEntry& b) {
                      return a.fingerprint < b.fingerprint;
                  });

        size_t ext_bits = totalBitsNeeded(ext_slots);
        if (ext_bits <= bucketDataBits()) {
            reencodeAllLSlots(*ext, ext_slots);
        } else {
            // Recursively handle overflow via addToChain for each entry.
            // This should be extremely rare.
            reencodeAllLSlots(*ext, ext_slots);
        }
        return true;
    }

    return false;
}

// --- Public read API ---

bool DeltaHashTable::findAllByHash(uint32_t bi, uint32_t li, uint64_t fp,
                                    std::vector<uint64_t>& packed_versions,
                                    std::vector<uint32_t>& ids) const {
    packed_versions.clear();
    ids.clear();

    const Bucket* bucket = &buckets_[bi];
    while (bucket) {
        LSlotContents contents = decodeLSlot(*bucket, li);
        for (const auto& entry : contents.entries) {
            if (entry.fingerprint == fp) {
                packed_versions.insert(packed_versions.end(),
                                       entry.packed_versions.begin(),
                                       entry.packed_versions.end());
                ids.insert(ids.end(), entry.ids.begin(), entry.ids.end());
            }
        }
        bucket = nextBucket(*bucket);
    }
    return !packed_versions.empty();
}

bool DeltaHashTable::findFirstByHash(uint32_t bi, uint32_t li, uint64_t fp,
                                      uint64_t& packed_version, uint32_t& id) const {
    bool found = false;
    uint64_t best_pv = 0;
    uint32_t best_id = 0;

    const Bucket* bucket = &buckets_[bi];
    while (bucket) {
        LSlotContents contents = decodeLSlot(*bucket, li);

        for (const auto& entry : contents.entries) {
            if (entry.fingerprint == fp && !entry.packed_versions.empty()) {
                uint64_t pv = entry.packed_versions[0];
                if (!found || pv > best_pv) {
                    best_pv = pv;
                    best_id = entry.ids[0];
                    found = true;
                }
            }
        }

        bucket = nextBucket(*bucket);
    }

    if (found) {
        packed_version = best_pv;
        id = best_id;
    }
    return found;
}

bool DeltaHashTable::findAll(std::string_view key,
                              std::vector<uint64_t>& packed_versions,
                              std::vector<uint32_t>& ids) const {
    uint64_t h = hashKey(key);
    return findAllByHash(bucketIndex(h), lslotIndex(h), fingerprint(h),
                         packed_versions, ids);
}

bool DeltaHashTable::findFirst(std::string_view key,
                                uint64_t& packed_version, uint32_t& id) const {
    uint64_t h = hashKey(key);
    return findFirstByHash(bucketIndex(h), lslotIndex(h), fingerprint(h),
                           packed_version, id);
}

bool DeltaHashTable::contains(std::string_view key) const {
    uint64_t pv;
    uint32_t id;
    return findFirst(key, pv, id);
}

void DeltaHashTable::forEach(
    const std::function<void(uint64_t hash, uint64_t packed_version,
                             uint32_t id)>& fn) const {
    uint32_t num_buckets = 1u << config_.bucket_bits;
    uint32_t n_lslots = numLSlots();

    for (uint32_t bi = 0; bi < num_buckets; ++bi) {
        const Bucket* b = &buckets_[bi];
        while (b) {
            size_t offset = 0;
            for (uint32_t s = 0; s < n_lslots; ++s) {
                LSlotContents contents =
                    lslot_codec_.decode(b->data, offset, &offset);
                for (const auto& entry : contents.entries) {
                    uint64_t hash =
                        (static_cast<uint64_t>(bi) << (64 - config_.bucket_bits)) |
                        (static_cast<uint64_t>(s) << fingerprint_bits_) |
                        entry.fingerprint;
                    for (size_t i = 0; i < entry.packed_versions.size(); ++i) {
                        fn(hash, entry.packed_versions[i], entry.ids[i]);
                    }
                }
            }
            b = nextBucket(*b);
        }
    }
}

void DeltaHashTable::forEachGroup(
    const std::function<void(uint64_t hash,
                             const std::vector<uint64_t>& packed_versions,
                             const std::vector<uint32_t>& ids)>& fn) const {
    uint32_t num_buckets = 1u << config_.bucket_bits;
    uint32_t n_lslots = numLSlots();

    for (uint32_t bi = 0; bi < num_buckets; ++bi) {
        struct Group {
            uint32_t lslot;
            uint64_t fp;
            std::vector<uint64_t> packed_versions;
            std::vector<uint32_t> ids;
        };
        // O(1) lookup by (lslot << fp_bits) | fingerprint
        std::unordered_map<uint64_t, Group> group_map;

        const Bucket* b = &buckets_[bi];
        while (b) {
            size_t offset = 0;
            for (uint32_t s = 0; s < n_lslots; ++s) {
                LSlotContents contents =
                    lslot_codec_.decode(b->data, offset, &offset);
                for (const auto& entry : contents.entries) {
                    uint64_t map_key = (static_cast<uint64_t>(s) << fingerprint_bits_)
                                       | entry.fingerprint;
                    auto it = group_map.find(map_key);
                    if (it != group_map.end()) {
                        it->second.packed_versions.insert(
                            it->second.packed_versions.end(),
                            entry.packed_versions.begin(),
                            entry.packed_versions.end());
                        it->second.ids.insert(
                            it->second.ids.end(),
                            entry.ids.begin(),
                            entry.ids.end());
                    } else {
                        group_map.emplace(map_key,
                            Group{s, entry.fingerprint,
                                  entry.packed_versions,
                                  entry.ids});
                    }
                }
            }
            b = nextBucket(*b);
        }

        for (const auto& [map_key, g] : group_map) {
            uint64_t hash =
                (static_cast<uint64_t>(bi) << (64 - config_.bucket_bits)) |
                (static_cast<uint64_t>(g.lslot) << fingerprint_bits_) |
                g.fp;
            fn(hash, g.packed_versions, g.ids);
        }
    }
}

// --- Stats ---

size_t DeltaHashTable::memoryUsage() const {
    size_t stride = config_.bucket_bytes + kBucketPadding;
    return buckets_.size() * stride + extensions_.size() * stride;
}

void DeltaHashTable::clearBuckets() {
    for (auto& bucket : buckets_) {
        initBucket(bucket);
    }
    extensions_.clear();
    ext_storage_.clear();
}

void DeltaHashTable::initBucket(Bucket& bucket) {
    size_t data_bytes = config_.bucket_bytes - 8;
    uint64_t ext_ptr = getExtensionPtr(bucket);
    std::memset(bucket.data, 0, data_bytes);
    setExtensionPtr(bucket, ext_ptr);

    BitWriter writer(bucket.data, 0);
    for (uint32_t s = 0; s < numLSlots(); ++s) {
        writer.writeUnary(0);
    }
}

}  // namespace internal
}  // namespace kvlite
