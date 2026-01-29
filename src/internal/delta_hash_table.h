#ifndef KVLITE_INTERNAL_DELTA_HASH_TABLE_H
#define KVLITE_INTERNAL_DELTA_HASH_TABLE_H

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace kvlite {
namespace internal {

// A record holding file_id list for a fingerprint slot.
// Stored externally; the DHT holds pointers to these.
struct KeyRecord {
    std::vector<uint32_t> file_ids;  // reverse-sorted by version (latest first)
};

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
// Target: ~9 bytes per key in the DHT itself (payload pointer dominates).
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

    // Insert a key, returning a pointer to its KeyRecord.
    // If the fingerprint already exists, returns the existing record.
    KeyRecord* insert(const std::string& key);

    // Insert by pre-computed hash (for snapshot loading).
    // Decomposes hash into bucket/lslot/fingerprint and find-or-creates.
    KeyRecord* insertByHash(uint64_t hash);

    // Find a key's record. Returns nullptr if not found.
    KeyRecord* find(const std::string& key) const;

    // Remove a key. Returns true if key was found and removed.
    bool remove(const std::string& key);

    // Iterate over all records, providing the reconstructed 64-bit hash.
    void forEach(const std::function<void(uint64_t hash, const KeyRecord&)>& fn) const;

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
        KeyRecord* record;
    };

    // An lslot's decoded contents: list of (fingerprint, record*) pairs.
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
    KeyRecord* findInChain(uint32_t bucket_idx, uint32_t lslot_idx,
                            uint64_t fp) const;

    // Insert into bucket chain, handling overflow
    KeyRecord* insertIntoChain(uint32_t bucket_idx, uint32_t lslot_idx,
                                uint64_t fp, KeyRecord* record);

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
    std::vector<std::unique_ptr<KeyRecord>> records_;
    std::mutex alloc_mutex_;    // protects records_ and extensions_
    std::atomic<size_t> size_{0};
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_DELTA_HASH_TABLE_H
