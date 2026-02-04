#ifndef KVLITE_INTERNAL_DELTA_HASH_TABLE_BASE_H
#define KVLITE_INTERNAL_DELTA_HASH_TABLE_BASE_H

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "internal/bit_stream.h"

namespace kvlite {
namespace internal {

// 64-bit FNV-1a with avalanche mixing.
inline uint64_t dhtHashBytes(const void* data, size_t len) {
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

// Base class for Delta Hash Tables, parameterized on codec type.
//
// Provides all bucket management, hash decomposition, extension chain
// handling, and generic chain-traversal helpers. Derived classes add
// value-type-specific public APIs and optional locking.
//
// Codec contract:
//   struct TrieEntry       { uint64_t fingerprint; ... };
//   struct LSlotContents   { std::vector<TrieEntry> entries; };
//   explicit Codec(uint8_t fingerprint_bits);
//   LSlotContents decode(const uint8_t*, size_t, size_t*) const;
//   size_t encode(uint8_t*, size_t, const LSlotContents&) const;
//   static size_t bitsNeeded(const LSlotContents&, uint8_t fp_bits);
template<typename Codec>
class DeltaHashTableBase {
public:
    using TrieEntry = typename Codec::TrieEntry;
    using LSlotContents = typename Codec::LSlotContents;

    struct Config {
        uint8_t bucket_bits = 20;
        uint8_t lslot_bits = 5;
        uint32_t bucket_bytes = 512;
    };

protected:
    static constexpr uint32_t kBucketPadding = 8;

    struct Bucket {
        std::vector<uint8_t> data;
        explicit Bucket(uint32_t size) : data(size + kBucketPadding, 0) {}
        Bucket() = default;
    };

    explicit DeltaHashTableBase(const Config& config)
        : config_(config),
          fingerprint_bits_(64 - config.bucket_bits - config.lslot_bits),
          lslot_codec_(fingerprint_bits_) {
        uint32_t num_buckets = 1u << config_.bucket_bits;
        buckets_.reserve(num_buckets);
        for (uint32_t i = 0; i < num_buckets; ++i) {
            buckets_.emplace_back(config_.bucket_bytes);
        }
        clearBuckets();
    }

    ~DeltaHashTableBase() = default;

    DeltaHashTableBase(const DeltaHashTableBase&) = delete;
    DeltaHashTableBase& operator=(const DeltaHashTableBase&) = delete;
    DeltaHashTableBase(DeltaHashTableBase&&) noexcept = default;
    DeltaHashTableBase& operator=(DeltaHashTableBase&&) noexcept = default;

    // --- Hash decomposition ---

    uint64_t hashKey(const std::string& key) const {
        return dhtHashBytes(key.data(), key.size());
    }

    uint32_t bucketIndex(uint64_t hash) const {
        return static_cast<uint32_t>(hash >> (64 - config_.bucket_bits));
    }

    uint32_t lslotIndex(uint64_t hash) const {
        return static_cast<uint32_t>(
            (hash >> (64 - config_.bucket_bits - config_.lslot_bits))
            & ((1u << config_.lslot_bits) - 1));
    }

    uint64_t fingerprint(uint64_t hash) const {
        return hash & ((1ULL << fingerprint_bits_) - 1);
    }

    // --- Bucket data ---

    uint64_t getExtensionPtr(const Bucket& bucket) const {
        uint64_t ptr = 0;
        std::memcpy(&ptr, bucket.data.data() + config_.bucket_bytes - 8, 8);
        return ptr;
    }

    void setExtensionPtr(Bucket& bucket, uint64_t ptr) const {
        std::memcpy(bucket.data.data() + config_.bucket_bytes - 8, &ptr, 8);
    }

    size_t bucketDataBits() const {
        return (config_.bucket_bytes - 8) * 8;
    }

    uint32_t numLSlots() const {
        return 1u << config_.lslot_bits;
    }

    // --- Decode / Encode ---

    // Decode a single lslot from a bucket by index.
    LSlotContents decodeLSlot(const Bucket& bucket, uint32_t lslot_idx) const {
        size_t bit_off = lslot_codec_.bitOffset(bucket.data.data(), lslot_idx);
        return lslot_codec_.decode(bucket.data.data(), bit_off);
    }

    std::vector<LSlotContents> decodeAllLSlots(const Bucket& bucket) const {
        uint32_t n = numLSlots();
        std::vector<LSlotContents> slots(n);
        size_t offset = 0;
        for (uint32_t s = 0; s < n; ++s) {
            slots[s] = lslot_codec_.decode(bucket.data.data(), offset, &offset);
        }
        return slots;
    }

    void reencodeAllLSlots(Bucket& bucket,
                           const std::vector<LSlotContents>& all_slots) {
        size_t data_bytes = config_.bucket_bytes - 8;
        uint64_t ext_ptr = getExtensionPtr(bucket);
        std::memset(bucket.data.data(), 0, data_bytes);
        setExtensionPtr(bucket, ext_ptr);

        size_t write_offset = 0;
        for (uint32_t s = 0; s < all_slots.size(); ++s) {
            write_offset = lslot_codec_.encode(bucket.data.data(), write_offset,
                                               all_slots[s]);
        }
    }

    size_t totalBitsNeeded(const std::vector<LSlotContents>& all_slots) const {
        size_t bits = 0;
        for (const auto& slot : all_slots) {
            bits += Codec::bitsNeeded(slot, fingerprint_bits_);
        }
        return bits;
    }

    // --- Extension chain ---

    const Bucket* nextBucket(const Bucket& bucket) const {
        uint64_t ext = getExtensionPtr(bucket);
        return ext ? extensions_[ext - 1].get() : nullptr;
    }

    Bucket* nextBucketMut(Bucket& bucket) {
        uint64_t ext = getExtensionPtr(bucket);
        return ext ? extensions_[ext - 1].get() : nullptr;
    }

    Bucket* createExtension(Bucket& bucket) {
        auto ext = std::make_unique<Bucket>(config_.bucket_bytes);
        extensions_.push_back(std::move(ext));
        uint64_t ext_ptr = extensions_.size();  // 1-based
        setExtensionPtr(bucket, ext_ptr);

        Bucket* ext_bucket = extensions_[ext_ptr - 1].get();
        initBucket(*ext_bucket);
        return ext_bucket;
    }

    // --- Generic chain helpers ---

    // Add to chain: decode → modify → check fit → re-encode or overflow.
    template<typename ModifyFn, typename UndoFn, typename CreateExtFn>
    void addToChainImpl(uint32_t bucket_idx, uint32_t lslot_idx,
                        ModifyFn&& modify, UndoFn&& undo,
                        CreateExtFn&& createExt) {
        Bucket* bucket = &buckets_[bucket_idx];

        while (true) {
            auto all_slots = decodeAllLSlots(*bucket);
            modify(all_slots[lslot_idx].entries);

            if (totalBitsNeeded(all_slots) <= bucketDataBits()) {
                reencodeAllLSlots(*bucket, all_slots);
                return;
            }

            undo(all_slots[lslot_idx].entries);

            Bucket* ext = nextBucketMut(*bucket);
            if (!ext) {
                ext = createExt(*bucket);
            }
            bucket = ext;
        }
    }

    // Find in chain: walk chain, collect from matching fingerprint entries.
    template<typename CollectFn>
    void findInChainImpl(uint32_t bucket_idx, uint32_t lslot_idx,
                         uint64_t fp, CollectFn&& collect) const {
        const Bucket* bucket = &buckets_[bucket_idx];
        while (bucket) {
            LSlotContents contents = decodeLSlot(*bucket, lslot_idx);
            for (const auto& entry : contents.entries) {
                if (entry.fingerprint == fp) {
                    collect(entry);
                }
            }
            bucket = nextBucket(*bucket);
        }
    }

    // Modify in chain: decode all → apply fn → re-encode if modified.
    template<typename ModifyFn>
    bool modifyInChainImpl(uint32_t bucket_idx, uint32_t lslot_idx,
                           ModifyFn&& fn) {
        Bucket* bucket = &buckets_[bucket_idx];
        while (bucket) {
            auto all_slots = decodeAllLSlots(*bucket);
            if (fn(all_slots[lslot_idx].entries)) {
                reencodeAllLSlots(*bucket, all_slots);
                return true;
            }
            bucket = nextBucketMut(*bucket);
        }
        return false;
    }

    // Walk all entries: for each (bucket, lslot, entry) invoke callback.
    template<typename EntryFn>
    void forEachEntryImpl(EntryFn&& fn) const {
        uint32_t num_buckets = 1u << config_.bucket_bits;
        uint32_t n_lslots = numLSlots();

        for (uint32_t bi = 0; bi < num_buckets; ++bi) {
            const Bucket* b = &buckets_[bi];
            while (b) {
                size_t offset = 0;
                for (uint32_t s = 0; s < n_lslots; ++s) {
                    LSlotContents contents =
                        lslot_codec_.decode(b->data.data(), offset, &offset);
                    for (const auto& entry : contents.entries) {
                        uint64_t hash =
                            (static_cast<uint64_t>(bi) << (64 - config_.bucket_bits)) |
                            (static_cast<uint64_t>(s) << fingerprint_bits_) |
                            entry.fingerprint;
                        fn(hash, entry);
                    }
                }
                b = nextBucket(*b);
            }
        }
    }

    // --- Stats ---

    size_t bucketMemoryUsage() const {
        return buckets_.size() * (config_.bucket_bytes + kBucketPadding)
             + extensions_.size() * (config_.bucket_bytes + kBucketPadding);
    }

    void clearBuckets() {
        for (auto& bucket : buckets_) {
            initBucket(bucket);
        }
        extensions_.clear();
    }

    // --- Members ---

    Config config_;
    uint8_t fingerprint_bits_;
    Codec lslot_codec_;
    std::vector<Bucket> buckets_;
    std::vector<std::unique_ptr<Bucket>> extensions_;

private:
    void initBucket(Bucket& bucket) {
        size_t data_bytes = config_.bucket_bytes - 8;
        uint64_t ext_ptr = getExtensionPtr(bucket);
        std::memset(bucket.data.data(), 0, data_bytes);
        setExtensionPtr(bucket, ext_ptr);

        BitWriter writer(bucket.data.data(), 0);
        for (uint32_t s = 0; s < numLSlots(); ++s) {
            writer.writeUnary(0);
        }
    }
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_DELTA_HASH_TABLE_BASE_H
