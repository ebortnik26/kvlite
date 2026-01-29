#ifndef KVLITE_INTERNAL_DELTA_HASH_TABLE_H
#define KVLITE_INTERNAL_DELTA_HASH_TABLE_H

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "internal/delta_hash_table_base.h"
#include "internal/lslot_codec.h"

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
// logical slots (lslots). Lslot encoding format is defined in LSlotCodec.
//
// When a bucket overflows, an extension bucket is chained.
// Thread-safe: per-bucket spinlocks protect concurrent access.
class DeltaHashTable : private DeltaHashTableBase<LSlotCodec> {
    using Base = DeltaHashTableBase<LSlotCodec>;

public:
    using Base::Config;
    using TrieEntry = LSlotCodec::TrieEntry;
    using LSlotContents = LSlotCodec::LSlotContents;

    DeltaHashTable();
    explicit DeltaHashTable(const Config& config);
    ~DeltaHashTable();

    // Non-copyable, non-movable (contains mutex and atomics)
    DeltaHashTable(const DeltaHashTable&) = delete;
    DeltaHashTable& operator=(const DeltaHashTable&) = delete;
    DeltaHashTable(DeltaHashTable&&) = delete;
    DeltaHashTable& operator=(DeltaHashTable&&) = delete;

    // Add a value for a key's fingerprint. Duplicate values are not added.
    void addEntry(const std::string& key, uint32_t value);

    // Add a value by pre-computed hash (for snapshot loading).
    void addEntryByHash(uint64_t hash, uint32_t value);

    // Find all values for a key. Returns true if key exists.
    // Values are ordered highest-first (latest id first).
    bool findAll(const std::string& key, std::vector<uint32_t>& out) const;

    // Find the first (highest/latest) value for a key. Returns true if found.
    bool findFirst(const std::string& key, uint32_t& value) const;

    // Check if a key exists.
    bool contains(const std::string& key) const;

    // Remove a specific value from a key. Returns true if found and removed.
    bool removeEntry(const std::string& key, uint32_t value);

    // Remove all values for a key. Returns number of values removed.
    size_t removeAll(const std::string& key);

    // Iterate over all entries, providing the reconstructed hash and each value.
    void forEach(const std::function<void(uint64_t hash, uint32_t value)>& fn) const;

    // Iterate over all groups, providing the reconstructed hash and all values.
    void forEachGroup(const std::function<void(uint64_t hash,
                                               const std::vector<uint32_t>&)>& fn) const;

    // Total number of id entries across all fingerprints.
    size_t size() const;
    size_t memoryUsage() const;
    void clear();

private:
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

    void addImpl(uint32_t bi, uint32_t li, uint64_t fp, uint32_t value);

    size_t removeAllFromChain(uint32_t bucket_idx, uint32_t lslot_idx,
                              uint64_t fp);

    std::unique_ptr<BucketLock[]> bucket_locks_;
    std::mutex ext_mutex_;
    std::atomic<size_t> size_{0};
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_DELTA_HASH_TABLE_H
