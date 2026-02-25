#ifndef KVLITE_INTERNAL_GLOBAL_DELTA_HASH_TABLE_H
#define KVLITE_INTERNAL_GLOBAL_DELTA_HASH_TABLE_H

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "internal/delta_hash_table_base.h"
#include "internal/segment_lslot_codec.h"

namespace kvlite {
namespace internal {

// Global Delta Hash Table: thread-safe compact hash table for the GlobalIndex.
//
// Stores (packed_version, segment_id) pairs per key fingerprint.
// Thread-safe: per-bucket spinlocks protect concurrent access.
//
// Uses SegmentLSlotCodec internally (identical encoding). The codec's
// field names (offsets/versions) are mapped to the public API's
// (packed_versions/segment_ids).
class GlobalDeltaHashTable : private DeltaHashTableBase<SegmentLSlotCodec> {
    using Base = DeltaHashTableBase<SegmentLSlotCodec>;

public:
    using Base::Config;
    using TrieEntry = SegmentLSlotCodec::TrieEntry;
    using LSlotContents = SegmentLSlotCodec::LSlotContents;

    GlobalDeltaHashTable();
    explicit GlobalDeltaHashTable(const Config& config);
    ~GlobalDeltaHashTable();

    // Non-copyable, non-movable (contains mutex and atomics)
    GlobalDeltaHashTable(const GlobalDeltaHashTable&) = delete;
    GlobalDeltaHashTable& operator=(const GlobalDeltaHashTable&) = delete;
    GlobalDeltaHashTable(GlobalDeltaHashTable&&) = delete;
    GlobalDeltaHashTable& operator=(GlobalDeltaHashTable&&) = delete;

    // Add a (packed_version, segment_id) pair for a key's fingerprint.
    void addEntry(const std::string& key, uint32_t packed_version, uint32_t segment_id);

    // Add by pre-computed hash (for snapshot loading).
    void addEntryByHash(uint64_t hash, uint32_t packed_version, uint32_t segment_id);

    // Find all (packed_version, segment_id) pairs for a key. Returns true if key exists.
    // Pairs are ordered by packed_version desc (latest first).
    bool findAll(const std::string& key,
                 std::vector<uint32_t>& packed_versions,
                 std::vector<uint32_t>& segment_ids) const;

    // Find the first (highest packed_version) entry. Returns true if found.
    bool findFirst(const std::string& key,
                   uint32_t& packed_version, uint32_t& segment_id) const;

    bool contains(const std::string& key) const;

    // Iterate over all entries.
    void forEach(const std::function<void(uint64_t hash,
                                          uint32_t packed_version,
                                          uint32_t segment_id)>& fn) const;

    // Iterate over all groups.
    void forEachGroup(const std::function<void(uint64_t hash,
                                               const std::vector<uint32_t>& packed_versions,
                                               const std::vector<uint32_t>& segment_ids)>& fn) const;

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

    void addImpl(uint32_t bi, uint32_t li, uint64_t fp,
                 uint32_t packed_version, uint32_t segment_id);

    std::unique_ptr<BucketLock[]> bucket_locks_;
    std::mutex ext_mutex_;
    std::atomic<size_t> size_{0};
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_GLOBAL_DELTA_HASH_TABLE_H
