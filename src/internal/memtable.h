#ifndef KVLITE_INTERNAL_MEMTABLE_H
#define KVLITE_INTERNAL_MEMTABLE_H

#include <atomic>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "internal/bucket_arena.h"
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
//   Data buffer  – contiguous, append-only byte array (sized to memtable_size).
//                  Each record: [key_len:2][value_len:4][packed_version:8][key][value]
//   Hash index   – contiguous array of 64K primary buckets (256 bytes each).
//                  Each bucket holds 12 packed Slots of {hash, pv, offset}.
//   Extensions   – BucketArena-allocated overflow buckets chained from full primaries.
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

    static constexpr size_t kDefaultDataCapacity = 256 * 1024 * 1024;  // 256 MB

    explicit Memtable(size_t data_capacity = kDefaultDataCapacity);
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

    bool get(uint64_t hash,
             std::string& value, uint64_t& version, bool& tombstone) const;

    bool getByVersion(uint64_t hash, uint64_t upper_bound,
                      std::string& value, uint64_t& version, bool& tombstone) const;

    // Called when writes are paused — no locking.
    void forEach(const std::function<void(uint64_t hash,
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
    uint32_t extensionCount() const { return ext_arena_.size(); }
    static constexpr uint32_t numBuckets() { return kNumBuckets; }
    bool empty() const { return size_.load(std::memory_order_relaxed) == 0; }

    // --- Record layout (data buffer) ---
    static constexpr size_t kKeyLenSize    = sizeof(uint16_t);  // 2
    static constexpr size_t kValueLenSize  = sizeof(uint32_t);  // 4
    static constexpr size_t kPackedVerSize = sizeof(uint64_t);  // 8
    static constexpr size_t kRecordHeaderSize = kKeyLenSize + kValueLenSize + kPackedVerSize;
    static constexpr size_t kValueLenOffset  = kKeyLenSize;
    static constexpr size_t kPackedVerOffset = kKeyLenSize + kValueLenSize;

private:
    // --- Slot: 20 bytes (packed) ---
    //
    // Full 64-bit hash stored so flush/createStream can pass it to
    // Segment::put / GlobalIndex without recomputation.
    struct __attribute__((packed)) Slot {
        uint64_t hash;     // full 64-bit hash
        uint64_t pv;       // PackedVersion raw data
        uint32_t offset;   // byte offset into data buffer
    };
    static_assert(sizeof(Slot) == 20, "Slot must be 20 bytes");

    // --- Bucket layout ---
    static constexpr uint32_t kBucketCountSize  = sizeof(uint16_t);  // 2
    static constexpr uint32_t kBucketExtPtrSize = sizeof(uint32_t);  // 4
    static constexpr uint32_t kBucketHeaderSize = kBucketCountSize + kBucketExtPtrSize;  // 6

    // --- Configuration ---
    static constexpr uint32_t kBucketBits = 16;
    static constexpr uint32_t kNumBuckets = 1u << kBucketBits;            // 65536
    static constexpr uint32_t kBucketBytes = 256;
    static constexpr uint32_t kSlotsPerBucket =
        (kBucketBytes - kBucketHeaderSize) / sizeof(Slot);                // 12
    static constexpr size_t kMinDataCapacity = 64 * 1024;                   // 64 KB floor

    // --- Data buffer (append-only) ---
    size_t data_capacity_;
    std::unique_ptr<uint8_t[]> data_;
    std::atomic<size_t> data_end_{0};

    // --- Primary buckets (contiguous, 16 MB) ---
    std::unique_ptr<uint8_t[]> buckets_;
    std::unique_ptr<Spinlock[]> locks_;

    // --- Extension buckets ---
    BucketArena ext_arena_;

    // --- Stats ---
    std::atomic<size_t> size_{0};
    std::atomic<size_t> key_count_{0};
    std::atomic<uint32_t> pin_count_{0};
    std::atomic<bool> sealed_{false};

    // --- Bucket accessors ---

    uint8_t* primaryBucket(uint32_t idx) {
        return buckets_.get() + static_cast<size_t>(idx) * kBucketBytes;
    }
    const uint8_t* primaryBucket(uint32_t idx) const {
        return buckets_.get() + static_cast<size_t>(idx) * kBucketBytes;
    }

    static uint16_t bucketCount(const uint8_t* b) {
        uint16_t v; std::memcpy(&v, b, kBucketCountSize); return v;
    }
    static void setBucketCount(uint8_t* b, uint16_t c) {
        std::memcpy(b, &c, kBucketCountSize);
    }
    static uint32_t bucketExtPtr(const uint8_t* b) {
        uint32_t v; std::memcpy(&v, b + kBucketCountSize, kBucketExtPtrSize); return v;
    }
    static void setBucketExtPtr(uint8_t* b, uint32_t e) {
        std::memcpy(b + kBucketCountSize, &e, kBucketExtPtrSize);
    }
    static Slot* bucketSlots(uint8_t* b) {
        return reinterpret_cast<Slot*>(b + kBucketHeaderSize);
    }
    static const Slot* bucketSlots(const uint8_t* b) {
        return reinterpret_cast<const Slot*>(b + kBucketHeaderSize);
    }

    uint8_t* extBucketData(uint32_t ext_ptr) {
        return ext_arena_.get(ext_ptr)->data;
    }
    const uint8_t* extBucketData(uint32_t ext_ptr) const {
        return ext_arena_.get(ext_ptr)->data;
    }

    // --- Hash decomposition ---
    uint32_t bucketIndex(uint64_t hash) const {
        return static_cast<uint32_t>(hash >> (64 - kBucketBits));
    }

    // --- Data buffer I/O ---
    uint32_t appendRecord(const std::string& key, PackedVersion pv,
                          const std::string& value);
    void readValue(uint32_t offset, std::string& value) const;
    std::string readKey(uint32_t offset) const;
};

} // namespace internal
} // namespace kvlite

#endif // KVLITE_INTERNAL_MEMTABLE_H
