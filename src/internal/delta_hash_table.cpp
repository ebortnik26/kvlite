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

DeltaHashTable::LSlotContents DeltaHashTable::decodeLSlot(
    const uint8_t* bucket_data, size_t bit_offset,
    size_t* end_bit_offset) const {

    BitReader reader(bucket_data, bit_offset);
    LSlotContents contents;

    uint64_t tenancy = reader.readUnary();

    contents.entries.resize(tenancy);
    for (uint64_t i = 0; i < tenancy; ++i) {
        contents.entries[i].fingerprint = reader.read(fingerprint_bits_);
    }

    for (uint64_t i = 0; i < tenancy; ++i) {
        uint64_t ptr_val = reader.read(64);
        contents.entries[i].record = reinterpret_cast<KeyRecord*>(ptr_val);
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

    uint64_t tenancy = contents.entries.size();

    writer.writeUnary(tenancy);

    for (uint64_t i = 0; i < tenancy; ++i) {
        writer.write(contents.entries[i].fingerprint, fingerprint_bits_);
    }

    for (uint64_t i = 0; i < tenancy; ++i) {
        uint64_t ptr_val = reinterpret_cast<uint64_t>(contents.entries[i].record);
        writer.write(ptr_val, 64);
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

// --- Lookup (bucket lock held by caller or by this method) ---

KeyRecord* DeltaHashTable::find(const std::string& key) const {
    uint64_t h = hashKey(key);
    uint32_t bi = bucketIndex(h);
    uint32_t li = lslotIndex(h);
    uint64_t fp = fingerprint(h);

    // Lock the primary bucket. The lock protects the entire chain.
    BucketLockGuard guard(const_cast<DeltaHashTable*>(this)->bucket_locks_[bi]);
    return findInChain(bi, li, fp, key);
}

KeyRecord* DeltaHashTable::findInChain(uint32_t bucket_idx, uint32_t lslot_idx,
                                        uint64_t fp, const std::string& key) const {
    const Bucket* bucket = &buckets_[bucket_idx];

    while (bucket) {
        size_t bit_off = lslotBitOffset(bucket->data.data(), lslot_idx);
        LSlotContents contents = decodeLSlot(bucket->data.data(), bit_off);

        for (const auto& entry : contents.entries) {
            if (entry.fingerprint == fp && entry.record->key == key) {
                return entry.record;
            }
        }

        uint64_t ext_ptr = getExtensionPtr(*bucket);
        if (ext_ptr == 0) {
            return nullptr;
        }
        bucket = extensions_[ext_ptr - 1].get();
    }
    return nullptr;
}

// --- Insert ---

KeyRecord* DeltaHashTable::insert(const std::string& key) {
    uint64_t h = hashKey(key);
    uint32_t bi = bucketIndex(h);
    uint32_t li = lslotIndex(h);
    uint64_t fp = fingerprint(h);

    // Lock the target bucket for the entire insert operation.
    BucketLockGuard guard(bucket_locks_[bi]);

    // Check if key already exists (under lock).
    KeyRecord* existing = findInChain(bi, li, fp, key);
    if (existing) {
        return existing;
    }

    // Allocate a new KeyRecord under the allocation lock.
    KeyRecord* record_ptr;
    {
        std::lock_guard<std::mutex> alloc_guard(alloc_mutex_);
        auto record = std::make_unique<KeyRecord>();
        record->key = key;
        record_ptr = record.get();
        records_.push_back(std::move(record));
    }

    insertIntoChain(bi, li, fp, record_ptr);
    size_.fetch_add(1, std::memory_order_relaxed);
    return record_ptr;
}

KeyRecord* DeltaHashTable::insertIntoChain(uint32_t bucket_idx,
                                            uint32_t lslot_idx,
                                            uint64_t fp,
                                            KeyRecord* record) {
    // Bucket lock is held by caller.
    Bucket* bucket = &buckets_[bucket_idx];

    while (true) {
        uint32_t num_lslots = 1u << config_.lslot_bits;

        // Decode all lslot contents.
        std::vector<LSlotContents> all_slots(num_lslots);
        size_t offset = 0;
        for (uint32_t s = 0; s < num_lslots; ++s) {
            all_slots[s] = decodeLSlot(bucket->data.data(), offset, &offset);
        }

        // Add the new entry to target lslot.
        TrieEntry new_entry;
        new_entry.fingerprint = fp;
        new_entry.record = record;
        all_slots[lslot_idx].entries.push_back(new_entry);

        // Sort entries by fingerprint within the lslot.
        std::sort(all_slots[lslot_idx].entries.begin(),
                  all_slots[lslot_idx].entries.end(),
                  [](const TrieEntry& a, const TrieEntry& b) {
                      return a.fingerprint < b.fingerprint;
                  });

        // Calculate total bits needed.
        size_t total_bits_needed = 0;
        for (uint32_t s = 0; s < num_lslots; ++s) {
            uint64_t tenancy = all_slots[s].entries.size();
            total_bits_needed += tenancy + 1;  // unary
            total_bits_needed += tenancy * fingerprint_bits_;
            total_bits_needed += tenancy * 64;  // pointers
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
            return record;
        }

        // Overflow: undo the addition and move to extension.
        auto& entries = all_slots[lslot_idx].entries;
        entries.erase(
            std::remove_if(entries.begin(), entries.end(),
                           [record](const TrieEntry& e) {
                               return e.record == record;
                           }),
            entries.end());

        // Follow or create extension bucket.
        uint64_t ext_ptr = getExtensionPtr(*bucket);
        if (ext_ptr == 0) {
            std::unique_ptr<Bucket> ext;
            {
                std::lock_guard<std::mutex> alloc_guard(alloc_mutex_);
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

bool DeltaHashTable::remove(const std::string& key) {
    uint64_t h = hashKey(key);
    uint32_t bi = bucketIndex(h);
    uint32_t li = lslotIndex(h);
    uint64_t fp = fingerprint(h);

    BucketLockGuard guard(bucket_locks_[bi]);

    if (removeFromChain(bi, li, fp, key)) {
        size_.fetch_sub(1, std::memory_order_relaxed);
        return true;
    }
    return false;
}

bool DeltaHashTable::removeFromChain(uint32_t bucket_idx, uint32_t lslot_idx,
                                      uint64_t fp, const std::string& key) {
    // Bucket lock is held by caller.
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
            if (it->fingerprint == fp && it->record->key == key) {
                entries.erase(it);
                found = true;
                break;
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
        if (ext_ptr == 0) {
            return false;
        }
        bucket = extensions_[ext_ptr - 1].get();
    }
    return false;
}

// --- Iteration ---
// Note: forEach/clear require external synchronization.
// They are not internally thread-safe with concurrent mutations.

void DeltaHashTable::forEach(
    const std::function<void(const KeyRecord&)>& fn) const {
    uint32_t num_lslots = 1u << config_.lslot_bits;

    auto scanBucket = [&](const Bucket* bucket) {
        size_t offset = 0;
        for (uint32_t s = 0; s < num_lslots; ++s) {
            LSlotContents contents = decodeLSlot(bucket->data.data(), offset,
                                                  &offset);
            for (const auto& entry : contents.entries) {
                fn(*entry.record);
            }
        }
    };

    for (const auto& bucket : buckets_) {
        const Bucket* b = &bucket;
        while (b) {
            scanBucket(b);
            uint64_t ext_ptr = getExtensionPtr(*b);
            if (ext_ptr == 0) break;
            b = extensions_[ext_ptr - 1].get();
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

    // KeyRecords
    for (const auto& rec : records_) {
        usage += sizeof(KeyRecord);
        usage += rec->key.capacity();
        usage += rec->entries.capacity() * sizeof(IndexEntry);
    }

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
    records_.clear();
    size_.store(0, std::memory_order_relaxed);
}

}  // namespace internal
}  // namespace kvlite
