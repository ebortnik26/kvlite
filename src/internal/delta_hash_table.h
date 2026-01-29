#ifndef KVLITE_INTERNAL_DELTA_HASH_TABLE_H
#define KVLITE_INTERNAL_DELTA_HASH_TABLE_H

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "id_vec_pool.h"

namespace kvlite {
namespace internal {

// Delta Hash Table: a compact hash table inspired by the Pliops XDP paper.
//
// Hash key decomposition:
//   [bucket_bits | lslot_bits | fingerprint_bits]
//   bucket_bits  → selects bucket
//   lslot_bits   → selects logical slot within bucket
//   fingerprint  → stored in delta trie for identity
//
// Each bucket is a fixed-size byte array containing up to 2^lslot_bits
// logical slots (lslots). Each lslot stores a variable number of entries
// using delta-encoded fingerprints in a trie structure.
//
// When a bucket overflows, an extension bucket is chained.
//
// Each entry stores a 64-bit payload (instead of a pointer). The payload
// is opaque to the DHT — callers encode inline file_ids or overflow
// KeyRecord pointers as they see fit.
class DeltaHashTable {
public:
    struct Config {
        uint8_t bucket_bits = 20;     // i: 2^20 = 1M buckets
        uint8_t lslot_bits = 5;       // j: 32 lslots per bucket
        uint32_t bucket_bytes = 512;  // fixed bucket size in bytes
    };

    DeltaHashTable();
    explicit DeltaHashTable(const Config& config);
    ~DeltaHashTable();

    // Non-copyable, non-movable (contains mutex and atomics)
    DeltaHashTable(const DeltaHashTable&) = delete;
    DeltaHashTable& operator=(const DeltaHashTable&) = delete;
    DeltaHashTable(DeltaHashTable&&) = delete;
    DeltaHashTable& operator=(DeltaHashTable&&) = delete;

    // Insert a key with an initial payload.
    // If the fingerprint already exists, returns {existing_payload, false}.
    // If new, inserts with initial_payload and returns {initial_payload, true}.
    std::pair<uint64_t, bool> insert(const std::string& key, uint64_t initial_payload);

    // Insert by pre-computed hash (for snapshot loading).
    // Same semantics as insert().
    std::pair<uint64_t, bool> insertByHash(uint64_t hash, uint64_t initial_payload);

    // Find a key's payload. Returns {payload, true} if found, {0, false} if not.
    std::pair<uint64_t, bool> find(const std::string& key) const;

    // Update the payload for an existing key. Returns true if key was found.
    bool updatePayload(const std::string& key, uint64_t new_payload);

    // Remove a key. Returns true if key was found and removed.
    bool remove(const std::string& key);

    // Iterate over all entries, providing the reconstructed 64-bit hash and payload.
    void forEach(const std::function<void(uint64_t hash, uint64_t payload)>& fn) const;

    // Access the DHT's id-vector pool. Callers use this to allocate/release
    // overflow vectors and to access their contents.
    IdVecPool& idVecPool() { return pool_; }
    const IdVecPool& idVecPool() const { return pool_; }

    size_t size() const;
    size_t memoryUsage() const;
    void clear();

private:
    // A bucket is a fixed-size byte array.
    // Layout: [lslot0_data | lslot1_data | ... | padding | ext_ptr(8 bytes)]
    // The last 8 bytes hold an extension pointer (index into extensions_,
    // 0 = no extension, 1-based index otherwise).
    // Extra bytes at end of each bucket for safe word-level bit I/O.
    // BitReader/BitWriter may load/store 8 bytes from any byte offset
    // within the data area; the padding prevents out-of-bounds access.
    static constexpr uint32_t kBucketPadding = 8;

    struct Bucket {
        std::vector<uint8_t> data;

        explicit Bucket(uint32_t size) : data(size + kBucketPadding, 0) {}
        Bucket() = default;
    };

    // Represents a single entry within an lslot's delta trie.
    // Used as intermediate representation for encode/decode.
    struct TrieEntry {
        uint64_t fingerprint;
        uint64_t payload;
    };

    // An lslot's decoded contents: list of (fingerprint, payload) pairs.
    struct LSlotContents {
        std::vector<TrieEntry> entries;
    };

    uint64_t hashKey(const std::string& key) const;

    // Extract components from hash
    uint32_t bucketIndex(uint64_t hash) const;
    uint32_t lslotIndex(uint64_t hash) const;
    uint64_t fingerprint(uint64_t hash) const;

    // Get the data offset (in bits) where lslot j starts in a bucket.
    // This requires scanning previous lslots (they're variable-width).
    // Returns the bit offset within the bucket data.
    size_t lslotBitOffset(const uint8_t* bucket_data, uint32_t target_lslot) const;

    // Decode an lslot at the given bit offset. Returns the contents and
    // the bit position after the lslot.
    LSlotContents decodeLSlot(const uint8_t* bucket_data, size_t bit_offset,
                               size_t* end_bit_offset = nullptr) const;

    // Encode an lslot's contents at the given bit offset.
    // Returns the bit position after the written data.
    size_t encodeLSlot(uint8_t* bucket_data, size_t bit_offset,
                       const LSlotContents& contents) const;

    // Get total bits used by all lslots in a bucket.
    size_t totalLSlotBits(const uint8_t* bucket_data) const;

    // Extension pointer management (stored in last 8 bytes of bucket)
    uint64_t getExtensionPtr(const Bucket& bucket) const;
    void setExtensionPtr(Bucket& bucket, uint64_t ptr) const;

    // Available data bits in a bucket (total bytes minus extension pointer)
    size_t bucketDataBits() const;

    // Lookup across bucket chain (fingerprint-only match)
    // Returns payload (0 if not found).
    uint64_t findInChain(uint32_t bucket_idx, uint32_t lslot_idx,
                          uint64_t fp) const;

    // Insert into bucket chain, handling overflow
    void insertIntoChain(uint32_t bucket_idx, uint32_t lslot_idx,
                          uint64_t fp, uint64_t payload);

    // Update payload in bucket chain. Returns true if found.
    bool updateInChain(uint32_t bucket_idx, uint32_t lslot_idx,
                        uint64_t fp, uint64_t new_payload);

    // Remove from bucket chain (fingerprint-only match)
    bool removeFromChain(uint32_t bucket_idx, uint32_t lslot_idx,
                          uint64_t fp);

    // Per-bucket spinlock. 1 byte each, contention is rare due to hashing.
    struct BucketLock {
        std::atomic<uint8_t> locked{0};

        void lock() {
            while (locked.exchange(1, std::memory_order_acquire)) {
                while (locked.load(std::memory_order_relaxed)) {
#if defined(__x86_64__) || defined(_M_X64)
                    __builtin_ia32_pause();
#endif
                }
            }
        }

        void unlock() {
            locked.store(0, std::memory_order_release);
        }
    };

    struct BucketLockGuard {
        BucketLock& lock_;
        explicit BucketLockGuard(BucketLock& l) : lock_(l) { lock_.lock(); }
        ~BucketLockGuard() { lock_.unlock(); }
        BucketLockGuard(const BucketLockGuard&) = delete;
        BucketLockGuard& operator=(const BucketLockGuard&) = delete;
    };

    Config config_;
    uint8_t fingerprint_bits_;
    std::vector<Bucket> buckets_;
    std::unique_ptr<BucketLock[]> bucket_locks_;
    std::vector<std::unique_ptr<Bucket>> extensions_;
    std::mutex ext_mutex_;      // protects extensions_
    IdVecPool pool_;
    std::atomic<size_t> size_{0};
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_DELTA_HASH_TABLE_H
