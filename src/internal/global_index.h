#ifndef KVLITE_INTERNAL_GLOBAL_INDEX_H
#define KVLITE_INTERNAL_GLOBAL_INDEX_H

#include <atomic>
#include <cstdint>
#include <functional>
#include <shared_mutex>
#include <string>
#include <vector>

#include "internal/read_write_delta_hash_table.h"
#include "kvlite/status.h"

namespace kvlite {
namespace internal {

class Manifest;
class SegmentStorageManager;

// GlobalIndex: In-memory index mapping keys to (segment_id, packed_version) lists,
// with built-in concurrency control and persistence.
//
// Structure: key -> [(segment_id, packed_version), ...]
//            sorted by packed_version desc (latest/highest first)
//
// Packed versions encode (logical_version << 1) | tombstone_bit.
//
// Backed by ReadWriteDeltaHashTable which stores (packed_version, id=segment_id) pairs.
//
// The GlobalIndex is always fully loaded in memory. It is persisted via:
// 1. Segment lineage sections (embedded in each segment file)
// 2. Periodic savepoints (full dump every N seconds + on shutdown)
//
// Recovery: load last savepoint, then replay lineage sections from all
// segments with ID > savepoint's max_segment_id watermark.
//
// Thread-safety: Index operations are thread-safe via per-bucket spinlocks
// in the underlying DeltaHashTable. Lifecycle methods (open/recover/close)
// must be called from a single thread.
//
// Savepoint locking: savepoint_mu_ blocks only GC (which removes/relocates
// entries). Flush (applyPut) runs concurrently with savepoints —
// per-bucket spinlocks ensure consistent bucket snapshots. BatchGuard is
// only needed for GC operations. Reads don't acquire savepoint_mu_.
class GlobalIndex {
public:
    struct Options {
        // Number of threads for savepoint write/load (1 = sequential)
        uint8_t savepoint_threads = 4;
    };

    // RAII guard that holds a shared lock on savepoint_mu_.
    // Only needed for GC merges (which remove/relocate entries).
    class BatchGuard {
    public:
        explicit BatchGuard(GlobalIndex& gi) : lock_(gi.savepoint_mu_) {}
        BatchGuard(const BatchGuard&) = delete;
        BatchGuard& operator=(const BatchGuard&) = delete;
    private:
        std::shared_lock<std::shared_mutex> lock_;
    };

    explicit GlobalIndex(Manifest& manifest);
    ~GlobalIndex();

    // Non-copyable
    GlobalIndex(const GlobalIndex&) = delete;
    GlobalIndex& operator=(const GlobalIndex&) = delete;

    // --- Lifecycle ---

    // Open and recover. SegmentStorageManager must already be open
    // (recovery reads lineage from segment files).
    Status open(const std::string& db_path, const Options& options,
                SegmentStorageManager& storage);
    Status close();
    bool isOpen() const;

    // --- Index Operations ---

    // Apply a put to the in-memory index. Thread-safe (per-bucket spinlocks).
    void applyPut(uint64_t hkey, uint64_t packed_version, uint32_t segment_id);

    // Batch-apply puts. Entries should be hash-sorted for optimal bucket
    // locality (one lock acquisition per bucket run).
    void applyPutBatch(const HashVersionPair* entries, size_t count,
                       uint32_t segment_id);

    bool get(uint64_t hkey,
             std::vector<uint32_t>& segment_ids,
             std::vector<uint64_t>& packed_versions) const;

    bool get(uint64_t hkey, uint64_t upper_bound,
             uint64_t& packed_version, uint32_t& segment_id) const;

    Status getLatest(uint64_t hkey,
                     uint64_t& packed_version, uint32_t& segment_id) const;

    bool contains(uint64_t hkey) const;

    // Update segment_id for an existing entry. Caller must hold a BatchGuard.
    void applyRelocate(uint64_t hkey, uint64_t packed_version,
                       uint32_t old_segment_id, uint32_t new_segment_id);

    // Remove an entry. Caller must hold a BatchGuard.
    void applyEliminate(uint64_t hkey, uint64_t packed_version,
                        uint32_t segment_id);

    // --- Iteration ---

    void forEachGroup(
        const std::function<void(uint64_t hash,
                                 const std::vector<uint64_t>& packed_versions,
                                 const std::vector<uint32_t>& segment_ids)>& fn) const;

    void clear();

    // --- Savepoint ---

    // Takes exclusive lock on savepoint_mu_, writes savepoint to disk,
    // persists max_segment_id to Manifest.
    Status storeSavepoint(uint32_t max_segment_id);

    // Create a savepoint if new segments have been processed since the last one.
    Status maybeSavepoint(uint32_t current_max_segment_id);

    // --- Statistics ---

    size_t keyCount() const;
    size_t entryCount() const;
    size_t memoryUsage() const;

    // Prune stale versions from the in-memory index. Keeps only versions
    // visible at the given snapshot observation points. Returns entries removed.
    size_t pruneStaleVersions(const std::vector<uint64_t>& snapshot_versions);

    // Average primary bucket utilization (0.0–1.0).
    double bucketUtilization() const;

    // Fraction of key groups with more than one version (0.0–1.0).
    double estimateDeadRatio() const;

    // DHT codec instrumentation
    uint64_t dhtEncodeCount() const { return dht_.encodeCount(); }
    uint64_t dhtEncodeTotalNs() const { return dht_.encodeTotalNs(); }
    uint64_t dhtDecodeCount() const { return dht_.decodeCount(); }
    uint64_t dhtDecodeTotalNs() const { return dht_.decodeTotalNs(); }
    uint32_t dhtExtCount() const { return dht_.extCount(); }
    uint32_t dhtNumBuckets() const { return dht_.numBuckets(); }

private:
    Status recover(SegmentStorageManager& storage);
    Status recoverPartialSwap(const std::string& valid_dir);
    Status replaySegmentLineages(SegmentStorageManager& storage, uint32_t after_segment_id);
    Status writeConvergenceSavepoint(const std::string& valid_dir, uint32_t max_seg_id);
    std::string savepointDir() const;

    // --- Data ---
    ReadWriteDeltaHashTable dht_;
    std::atomic<size_t> key_count_{0};

    // --- Concurrency ---
    mutable std::shared_mutex savepoint_mu_;

    // --- Lifecycle / persistence ---
    Manifest& manifest_;
    std::string db_path_;
    Options options_;
    bool is_open_ = false;
    uint32_t savepoint_max_segment_id_ = 0;
};

} // namespace internal
} // namespace kvlite

#endif // KVLITE_INTERNAL_GLOBAL_INDEX_H
