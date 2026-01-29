#include "internal/delta_hash_table.h"
#include "internal/bit_stream.h"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <functional>

namespace kvlite {
namespace internal {

// --- Hash function (64-bit FNV-1a with avalanche) ---

static uint64_t hashBytes(const void* data, size_t len) {
    uint64_t hash = 14695981039346656037ULL;
    const uint8_t* bytes = static_cast<const uint8_t*>(data);
    for (size_t i = 0; i < len; ++i) {
        hash ^= bytes[i];
        hash *= 1099511628211ULL;
    }
    hash ^= hash >> 33;
    hash *= 0xff51afd7ed558ccdULL;
    hash ^= hash >> 33;
    hash *= 0xc4ceb9fe1a85ec53ULL;
    hash ^= hash >> 33;
    return hash;
}

// --- DeltaHashTable Implementation ---

DeltaHashTable::DeltaHashTable() : DeltaHashTable(Config{}) {}

DeltaHashTable::DeltaHashTable(const Config& config)
    : config_(config),
      fingerprint_bits_(64 - config.bucket_bits - config.lslot_bits) {
    uint32_t num_buckets = 1u << config_.bucket_bits;
    buckets_.reserve(num_buckets);
    for (uint32_t i = 0; i < num_buckets; ++i) {
        buckets_.emplace_back(config_.bucket_bytes);
    }
    bucket_locks_ = std::make_unique<BucketLock[]>(num_buckets);
}

DeltaHashTable::~DeltaHashTable() = default;


uint64_t DeltaHashTable::hashKey(const std::string& key) const {
    return hashBytes(key.data(), key.size());
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

size_t DeltaHashTable::bucketDataBits() const {
    return (config_.bucket_bytes - 8) * 8;
}

uint64_t DeltaHashTable::getExtensionPtr(const Bucket& bucket) const {
    uint64_t ptr = 0;
    size_t offset = config_.bucket_bytes - 8;
    std::memcpy(&ptr, bucket.data.data() + offset, 8);
    return ptr;
}

void DeltaHashTable::setExtensionPtr(Bucket& bucket, uint64_t ptr) const {
    size_t offset = config_.bucket_bytes - 8;
    std::memcpy(bucket.data.data() + offset, &ptr, 8);
}

// --- LSlot Encoding ---
//
// Format per lslot:
//   unary(K)                       — K unique fingerprints
//   for each fingerprint:
//     [fingerprint_bits_]          — fingerprint value
//     unary(M)                     — M values for this fingerprint
//     M × [32 bits]                — the id values

DeltaHashTable::LSlotContents DeltaHashTable::decodeLSlot(
    const uint8_t* bucket_data, size_t bit_offset,
    size_t* end_bit_offset) const {

    BitReader reader(bucket_data, bit_offset);
    LSlotContents contents;

    uint64_t num_fps = reader.readUnary();
    contents.entries.resize(num_fps);

    for (uint64_t i = 0; i < num_fps; ++i) {
        contents.entries[i].fingerprint = reader.read(fingerprint_bits_);
        uint64_t num_values = reader.readUnary();
        contents.entries[i].values.resize(num_values);
        for (uint64_t v = 0; v < num_values; ++v) {
            contents.entries[i].values[v] = static_cast<uint32_t>(reader.read(32));
        }
    }

    if (end_bit_offset) {
        *end_bit_offset = reader.position();
    }
    return contents;
}

size_t DeltaHashTable::encodeLSlot(
    uint8_t* bucket_data, size_t bit_offset,
    const LSlotContents& contents) const {

    BitWriter writer(bucket_data, bit_offset);

    uint64_t num_fps = contents.entries.size();
    writer.writeUnary(num_fps);

    for (uint64_t i = 0; i < num_fps; ++i) {
        writer.write(contents.entries[i].fingerprint, fingerprint_bits_);
        uint64_t num_values = contents.entries[i].values.size();
        writer.writeUnary(num_values);
        for (uint64_t v = 0; v < num_values; ++v) {
            writer.write(contents.entries[i].values[v], 32);
        }
    }

    return writer.position();
}

size_t DeltaHashTable::lslotBitOffset(const uint8_t* bucket_data,
                                       uint32_t target_lslot) const {
    size_t offset = 0;
    for (uint32_t s = 0; s < target_lslot; ++s) {
        decodeLSlot(bucket_data, offset, &offset);
    }
    return offset;
}

size_t DeltaHashTable::totalLSlotBits(const uint8_t* bucket_data) const {
    uint32_t num_lslots = 1u << config_.lslot_bits;
    return lslotBitOffset(bucket_data, num_lslots);
}

// --- Calculate bits needed for an lslot ---

size_t DeltaHashTable::lslotBitsNeeded(const LSlotContents& contents,
                                        uint8_t fp_bits) {
    size_t bits = contents.entries.size() + 1;  // unary(K)
    for (const auto& entry : contents.entries) {
        bits += fp_bits;                         // fingerprint
        bits += entry.values.size() + 1;         // unary(M)
        bits += entry.values.size() * 32;        // M × 32-bit values
    }
    return bits;
}

// --- Find ---

bool DeltaHashTable::findAll(const std::string& key,
                              std::vector<uint32_t>& out) const {
    uint64_t h = hashKey(key);
    uint32_t bi = bucketIndex(h);
    uint32_t li = lslotIndex(h);
    uint64_t fp = fingerprint(h);

    BucketLockGuard guard(const_cast<DeltaHashTable*>(this)->bucket_locks_[bi]);
    out.clear();
    findAllInChain(bi, li, fp, out);
    return !out.empty();
}

bool DeltaHashTable::findFirst(const std::string& key, uint32_t& value) const {
    uint64_t h = hashKey(key);
    uint32_t bi = bucketIndex(h);
    uint32_t li = lslotIndex(h);
    uint64_t fp = fingerprint(h);

    BucketLockGuard guard(const_cast<DeltaHashTable*>(this)->bucket_locks_[bi]);

    // Scan chain for first match — values are stored desc, so first found is highest.
    const Bucket* bucket = &buckets_[bi];
    while (bucket) {
        size_t bit_off = lslotBitOffset(bucket->data.data(), li);
        LSlotContents contents = decodeLSlot(bucket->data.data(), bit_off);

        for (const auto& entry : contents.entries) {
            if (entry.fingerprint == fp && !entry.values.empty()) {
                value = entry.values[0];
                return true;
            }
        }

        uint64_t ext_ptr = getExtensionPtr(*bucket);
        if (ext_ptr == 0) return false;
        bucket = extensions_[ext_ptr - 1].get();
    }
    return false;
}

bool DeltaHashTable::contains(const std::string& key) const {
    uint32_t dummy;
    return findFirst(key, dummy);
}

void DeltaHashTable::findAllInChain(uint32_t bucket_idx, uint32_t lslot_idx,
                                     uint64_t fp,
                                     std::vector<uint32_t>& out) const {
    const Bucket* bucket = &buckets_[bucket_idx];

    while (bucket) {
        size_t bit_off = lslotBitOffset(bucket->data.data(), lslot_idx);
        LSlotContents contents = decodeLSlot(bucket->data.data(), bit_off);

        for (const auto& entry : contents.entries) {
            if (entry.fingerprint == fp) {
                out.insert(out.end(), entry.values.begin(), entry.values.end());
            }
        }

        uint64_t ext_ptr = getExtensionPtr(*bucket);
        if (ext_ptr == 0) return;
        bucket = extensions_[ext_ptr - 1].get();
    }
}

// --- Add ---

void DeltaHashTable::addEntry(const std::string& key, uint32_t value) {
    uint64_t h = hashKey(key);
    uint32_t bi = bucketIndex(h);
    uint32_t li = lslotIndex(h);
    uint64_t fp = fingerprint(h);

    BucketLockGuard guard(bucket_locks_[bi]);

    // Check for duplicate across chain
    {
        const Bucket* bucket = &buckets_[bi];
        while (bucket) {
            size_t bit_off = lslotBitOffset(bucket->data.data(), li);
            LSlotContents contents = decodeLSlot(bucket->data.data(), bit_off);
            for (const auto& entry : contents.entries) {
                if (entry.fingerprint == fp) {
                    for (uint32_t v : entry.values) {
                        if (v == value) return;  // duplicate
                    }
                }
            }
            uint64_t ext_ptr = getExtensionPtr(*bucket);
            if (ext_ptr == 0) break;
            bucket = extensions_[ext_ptr - 1].get();
        }
    }

    addToChain(bi, li, fp, value);
    size_.fetch_add(1, std::memory_order_relaxed);
}

void DeltaHashTable::addEntryByHash(uint64_t hash, uint32_t value) {
    uint32_t bi = bucketIndex(hash);
    uint32_t li = lslotIndex(hash);
    uint64_t fp = fingerprint(hash);

    BucketLockGuard guard(bucket_locks_[bi]);

    // Check for duplicate across chain
    {
        const Bucket* bucket = &buckets_[bi];
        while (bucket) {
            size_t bit_off = lslotBitOffset(bucket->data.data(), li);
            LSlotContents contents = decodeLSlot(bucket->data.data(), bit_off);
            for (const auto& entry : contents.entries) {
                if (entry.fingerprint == fp) {
                    for (uint32_t v : entry.values) {
                        if (v == value) return;  // duplicate
                    }
                }
            }
            uint64_t ext_ptr = getExtensionPtr(*bucket);
            if (ext_ptr == 0) break;
            bucket = extensions_[ext_ptr - 1].get();
        }
    }

    addToChain(bi, li, fp, value);
    size_.fetch_add(1, std::memory_order_relaxed);
}

void DeltaHashTable::addToChain(uint32_t bucket_idx, uint32_t lslot_idx,
                                 uint64_t fp, uint32_t value) {
    Bucket* bucket = &buckets_[bucket_idx];

    while (true) {
        uint32_t num_lslots = 1u << config_.lslot_bits;

        // Decode all lslot contents.
        std::vector<LSlotContents> all_slots(num_lslots);
        size_t offset = 0;
        for (uint32_t s = 0; s < num_lslots; ++s) {
            all_slots[s] = decodeLSlot(bucket->data.data(), offset, &offset);
        }

        // Find or create the fingerprint group in the target lslot.
        auto& entries = all_slots[lslot_idx].entries;
        bool found_fp = false;
        for (auto& entry : entries) {
            if (entry.fingerprint == fp) {
                // Insert value in desc order (highest first)
                auto it = std::lower_bound(entry.values.begin(), entry.values.end(),
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
            // Keep entries sorted by fingerprint asc
            std::sort(entries.begin(), entries.end(),
                      [](const TrieEntry& a, const TrieEntry& b) {
                          return a.fingerprint < b.fingerprint;
                      });
        }

        // Calculate total bits needed.
        size_t total_bits_needed = 0;
        for (uint32_t s = 0; s < num_lslots; ++s) {
            total_bits_needed += lslotBitsNeeded(all_slots[s], fingerprint_bits_);
        }

        size_t available_bits = bucketDataBits();

        if (total_bits_needed <= available_bits) {
            // Fits. Zero data area and re-encode.
            size_t data_bytes = config_.bucket_bytes - 8;
            uint64_t ext_ptr = getExtensionPtr(*bucket);
            std::memset(bucket->data.data(), 0, data_bytes);
            setExtensionPtr(*bucket, ext_ptr);

            size_t write_offset = 0;
            for (uint32_t s = 0; s < num_lslots; ++s) {
                write_offset = encodeLSlot(bucket->data.data(), write_offset,
                                           all_slots[s]);
            }
            return;
        }

        // Overflow: undo the addition and move to extension.
        // Remove the value we just added from the target lslot.
        for (auto it = entries.begin(); it != entries.end(); ++it) {
            if (it->fingerprint == fp) {
                auto vit = std::find(it->values.begin(), it->values.end(), value);
                if (vit != it->values.end()) {
                    it->values.erase(vit);
                }
                if (it->values.empty()) {
                    entries.erase(it);
                }
                break;
            }
        }

        // Follow or create extension bucket.
        uint64_t ext_ptr = getExtensionPtr(*bucket);
        if (ext_ptr == 0) {
            std::unique_ptr<Bucket> ext;
            {
                std::lock_guard<std::mutex> ext_guard(ext_mutex_);
                ext = std::make_unique<Bucket>(config_.bucket_bytes);
                extensions_.push_back(std::move(ext));
                ext_ptr = extensions_.size();  // 1-based
            }
            setExtensionPtr(*bucket, ext_ptr);

            // Initialize extension with empty lslots.
            Bucket* ext_bucket = extensions_[ext_ptr - 1].get();
            BitWriter writer(ext_bucket->data.data(), 0);
            for (uint32_t s = 0; s < (1u << config_.lslot_bits); ++s) {
                writer.writeUnary(0);
            }
        }
        bucket = extensions_[ext_ptr - 1].get();
    }
}

// --- Remove ---

bool DeltaHashTable::removeEntry(const std::string& key, uint32_t value) {
    uint64_t h = hashKey(key);
    uint32_t bi = bucketIndex(h);
    uint32_t li = lslotIndex(h);
    uint64_t fp = fingerprint(h);

    BucketLockGuard guard(bucket_locks_[bi]);

    if (removeValueFromChain(bi, li, fp, value)) {
        size_.fetch_sub(1, std::memory_order_relaxed);
        return true;
    }
    return false;
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

bool DeltaHashTable::removeValueFromChain(uint32_t bucket_idx,
                                           uint32_t lslot_idx,
                                           uint64_t fp, uint32_t value) {
    Bucket* bucket = &buckets_[bucket_idx];

    while (bucket) {
        uint32_t num_lslots = 1u << config_.lslot_bits;

        std::vector<LSlotContents> all_slots(num_lslots);
        size_t offset = 0;
        for (uint32_t s = 0; s < num_lslots; ++s) {
            all_slots[s] = decodeLSlot(bucket->data.data(), offset, &offset);
        }

        auto& entries = all_slots[lslot_idx].entries;
        bool found = false;
        for (auto it = entries.begin(); it != entries.end(); ++it) {
            if (it->fingerprint == fp) {
                auto vit = std::find(it->values.begin(), it->values.end(), value);
                if (vit != it->values.end()) {
                    it->values.erase(vit);
                    if (it->values.empty()) {
                        entries.erase(it);
                    }
                    found = true;
                    break;
                }
            }
        }

        if (found) {
            size_t data_bytes = config_.bucket_bytes - 8;
            uint64_t ext_ptr = getExtensionPtr(*bucket);
            std::memset(bucket->data.data(), 0, data_bytes);
            setExtensionPtr(*bucket, ext_ptr);

            size_t write_offset = 0;
            for (uint32_t s = 0; s < num_lslots; ++s) {
                write_offset = encodeLSlot(bucket->data.data(), write_offset,
                                           all_slots[s]);
            }
            return true;
        }

        uint64_t ext_ptr = getExtensionPtr(*bucket);
        if (ext_ptr == 0) return false;
        bucket = extensions_[ext_ptr - 1].get();
    }
    return false;
}

size_t DeltaHashTable::removeAllFromChain(uint32_t bucket_idx,
                                           uint32_t lslot_idx,
                                           uint64_t fp) {
    Bucket* bucket = &buckets_[bucket_idx];
    size_t total_removed = 0;

    while (bucket) {
        uint32_t num_lslots = 1u << config_.lslot_bits;

        std::vector<LSlotContents> all_slots(num_lslots);
        size_t offset = 0;
        for (uint32_t s = 0; s < num_lslots; ++s) {
            all_slots[s] = decodeLSlot(bucket->data.data(), offset, &offset);
        }

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

            size_t data_bytes = config_.bucket_bytes - 8;
            uint64_t ext_ptr = getExtensionPtr(*bucket);
            std::memset(bucket->data.data(), 0, data_bytes);
            setExtensionPtr(*bucket, ext_ptr);

            size_t write_offset = 0;
            for (uint32_t s = 0; s < num_lslots; ++s) {
                write_offset = encodeLSlot(bucket->data.data(), write_offset,
                                           all_slots[s]);
            }
        }

        uint64_t ext_ptr = getExtensionPtr(*bucket);
        if (ext_ptr == 0) break;
        bucket = extensions_[ext_ptr - 1].get();
    }

    return total_removed;
}

// --- Iteration ---

void DeltaHashTable::forEach(
    const std::function<void(uint64_t hash, uint32_t value)>& fn) const {
    uint32_t num_buckets = 1u << config_.bucket_bits;
    uint32_t num_lslots = 1u << config_.lslot_bits;

    auto scanBucket = [&](uint32_t bucket_idx, const Bucket* bucket) {
        size_t offset = 0;
        for (uint32_t s = 0; s < num_lslots; ++s) {
            LSlotContents contents = decodeLSlot(bucket->data.data(), offset,
                                                  &offset);
            for (const auto& entry : contents.entries) {
                uint64_t hash =
                    (static_cast<uint64_t>(bucket_idx) << (64 - config_.bucket_bits)) |
                    (static_cast<uint64_t>(s) << fingerprint_bits_) |
                    entry.fingerprint;
                for (uint32_t v : entry.values) {
                    fn(hash, v);
                }
            }
        }
    };

    for (uint32_t bi = 0; bi < num_buckets; ++bi) {
        const Bucket* b = &buckets_[bi];
        while (b) {
            scanBucket(bi, b);
            uint64_t ext_ptr = getExtensionPtr(*b);
            if (ext_ptr == 0) break;
            b = extensions_[ext_ptr - 1].get();
        }
    }
}

void DeltaHashTable::forEachGroup(
    const std::function<void(uint64_t hash, const std::vector<uint32_t>&)>& fn) const {
    uint32_t num_buckets = 1u << config_.bucket_bits;
    uint32_t num_lslots = 1u << config_.lslot_bits;

    // For each bucket chain, collect all values per (lslot, fingerprint) across
    // the chain, then call fn once per group.
    for (uint32_t bi = 0; bi < num_buckets; ++bi) {
        // Collect groups: map from (lslot_idx, fingerprint) -> merged values
        // Using a flat structure since typically few entries per chain.
        struct Group {
            uint32_t lslot;
            uint64_t fp;
            std::vector<uint32_t> values;
        };
        std::vector<Group> groups;

        const Bucket* b = &buckets_[bi];
        while (b) {
            size_t offset = 0;
            for (uint32_t s = 0; s < num_lslots; ++s) {
                LSlotContents contents = decodeLSlot(b->data.data(), offset,
                                                      &offset);
                for (const auto& entry : contents.entries) {
                    // Find existing group or create new
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
            uint64_t ext_ptr = getExtensionPtr(*b);
            if (ext_ptr == 0) break;
            b = extensions_[ext_ptr - 1].get();
        }

        // Emit groups
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
    size_t usage = 0;

    // Primary buckets (including padding)
    usage += buckets_.size() * (config_.bucket_bytes + kBucketPadding);

    // Bucket locks
    usage += (1u << config_.bucket_bits) * sizeof(BucketLock);

    // Extension buckets
    usage += extensions_.size() * (config_.bucket_bytes + kBucketPadding);

    return usage;
}

void DeltaHashTable::clear() {
    for (auto& bucket : buckets_) {
        std::memset(bucket.data.data(), 0, config_.bucket_bytes);
        BitWriter writer(bucket.data.data(), 0);
        uint32_t num_lslots = 1u << config_.lslot_bits;
        for (uint32_t s = 0; s < num_lslots; ++s) {
            writer.writeUnary(0);
        }
    }

    extensions_.clear();
    size_.store(0, std::memory_order_relaxed);
}

}  // namespace internal
}  // namespace kvlite
