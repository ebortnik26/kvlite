#ifndef KVLITE_INTERNAL_MEMTABLE_H
#define KVLITE_INTERNAL_MEMTABLE_H

#include <atomic>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "internal/entry_stream.h"
#include "internal/spinlock.h"
#include "log_entry.h"
#include "kvlite/status.h"

namespace kvlite {
namespace internal {

class Segment;

// In-memory buffer for pending writes before flush to log files.
//
// Layout:
//   Data buffer  – contiguous, append-only byte array (1 GB default).
//                  Each record: [key_len:2][value_len:4][packed_version:8][key][value]
//   Hash index   – contiguous array of 2^k buckets (k=13 → 8192 buckets).
//                  Each bucket holds fixed-size slots of {fingerprint, offset}.
//   Overflow     – block-allocated overflow buckets chained from full primaries.
//
// Thread-safety: Per-bucket spinlocks protect concurrent put/get operations
// while the Memtable is mutable. After seal(), the Memtable becomes read-only
// and all read operations skip spinlock acquisition.
class Memtable {
public:
    struct Entry {
        PackedVersion pv;
        std::string value;

        uint64_t version() const { return pv.version(); }
        bool tombstone() const { return pv.tombstone(); }

        bool operator<(const Entry& other) const { return pv < other.pv; }
    };

    Memtable();
    ~Memtable();

    Memtable(const Memtable&) = delete;
    Memtable& operator=(const Memtable&) = delete;

    void put(const std::string& key, uint64_t version,
             const std::string& value, bool tombstone);

    // Atomic batch insert: all operations share the same version.
    // Two-phase locking with bucket-ordered acquisition prevents deadlocks.
    struct BatchOp {
        const std::string* key;
        const std::string* value;
        bool tombstone;
    };
    void putBatch(const std::vector<BatchOp>& ops, uint64_t version);

    bool get(const std::string& key,
             std::string& value, uint64_t& version, bool& tombstone) const;

    bool getByVersion(const std::string& key, uint64_t upper_bound,
                      std::string& value, uint64_t& version, bool& tombstone) const;

    // Called when writes are paused — no locking.
    void forEach(const std::function<void(const std::string&,
                                          const std::vector<Entry>&)>& fn) const;

    void clear();

    struct FlushedEntry { uint64_t hkey; uint64_t packed_ver; };

    struct FlushResult {
        Status status;
        std::vector<FlushedEntry> entries;
        uint64_t max_version = 0;  // max logical version across all flushed entries
    };

    // Flush entries to a Segment that is already in Writing state.
    // Deduplicates per key using the same two-pointer dedup algorithm
    // as GC: for each snapshot version, the latest entry version <=
    // snapshot is kept. Entries are sorted by (hash, key) ascending.
    // Returns (key, packed_ver) pairs for GlobalIndex staging.
    //
    // snapshot_versions: all observation points (active snapshots +
    // latestVersion), sorted ascending. Empty = eliminate all redundant
    // versions.
    FlushResult flush(Segment& out,
                      const std::vector<uint64_t>& snapshot_versions = {});

    // Create a stream of entries visible at snapshot_version.
    // Thread-safe: locks each bucket during collection.
    // Returns entries sorted by (hash asc, version asc), deduplicated
    // per key (only the latest version <= snapshot_version).
    std::unique_ptr<EntryStream> createStream(uint64_t snapshot_version) const;

    // Mark this Memtable as immutable. After seal(), reads skip spinlocks.
    // Must not be called concurrently with put/putBatch.
    void seal() { sealed_.store(true, std::memory_order_release); }
    bool isSealed() const { return sealed_.load(std::memory_order_acquire); }

    void pin() { pin_count_.fetch_add(1, std::memory_order_relaxed); }
    void unpin() { pin_count_.fetch_sub(1, std::memory_order_release); }
    uint32_t pinCount() const { return pin_count_.load(std::memory_order_acquire); }

    const uint8_t* dataPtr() const { return data_.get(); }

    size_t keyCount() const { return key_count_.load(std::memory_order_relaxed); }
    size_t entryCount() const { return size_.load(std::memory_order_relaxed); }
    size_t memoryUsage() const { return data_end_.load(std::memory_order_relaxed); }
    bool empty() const { return size_.load(std::memory_order_relaxed) == 0; }

private:
    // --- Configuration ---
    static constexpr uint32_t kBucketBits = 13;
    static constexpr uint32_t kNumBuckets = 1u << kBucketBits;          // 8192
    static constexpr size_t kDefaultDataCapacity = 1ULL << 30;          // 1 GB
    static constexpr uint32_t kSlotsPerBucket = 63;
    static constexpr size_t kRecordHeaderSize = 14;  // key_len(2) + value_len(4) + pv(8)

    // Overflow pool block sizing
    static constexpr uint32_t kOverflowBlockShift = 10;
    static constexpr uint32_t kOverflowBlockSize = 1u << kOverflowBlockShift;  // 1024
    static constexpr uint32_t kMaxOverflowBlocks = 256;  // up to 256K overflow buckets

    // --- Hash index structures ---

    struct Slot {
        uint32_t fingerprint;  // lower 32 bits of hash (fast rejection)
        uint32_t offset;       // byte offset into data buffer
    };

    struct Bucket {
        uint32_t count;     // used slots in this bucket
        uint32_t overflow;  // 1-based overflow bucket index, 0 = none
        Slot slots[kSlotsPerBucket];
    };
    // sizeof(Bucket) = 8 + 63*8 = 512

    // --- Data buffer (append-only) ---
    std::unique_ptr<uint8_t[]> data_;
    size_t data_capacity_;
    std::atomic<size_t> data_end_{0};

    // --- Hash index (contiguous) ---
    std::unique_ptr<Bucket[]> buckets_;
    std::unique_ptr<Spinlock[]> locks_;

    // --- Overflow pool (block-allocated) ---
    Bucket* overflow_blocks_[kMaxOverflowBlocks];
    std::atomic<uint32_t> overflow_count_{0};
    Spinlock overflow_alloc_lock_;

    // --- Stats ---
    std::atomic<size_t> size_{0};
    std::atomic<size_t> key_count_{0};
    std::atomic<uint32_t> pin_count_{0};
    std::atomic<bool> sealed_{false};

    // --- Helpers ---

    uint32_t bucketIndex(uint64_t hash) const {
        return static_cast<uint32_t>(hash >> (64 - kBucketBits));
    }

    uint32_t fingerprint(uint64_t hash) const {
        return static_cast<uint32_t>(hash);
    }

    uint32_t appendRecord(const std::string& key, PackedVersion pv,
                          const std::string& value);

    bool keyMatches(uint32_t offset, const std::string& key) const;
    PackedVersion readPackedVersion(uint32_t offset) const;
    void readValue(uint32_t offset, std::string& value) const;
    std::string readKey(uint32_t offset) const;

    Bucket& getOverflowBucket(uint32_t idx);
    const Bucket& getOverflowBucket(uint32_t idx) const;
    uint32_t allocOverflowBucket();
    void freeOverflow();
};

} // namespace internal
} // namespace kvlite

#endif // KVLITE_INTERNAL_MEMTABLE_H
