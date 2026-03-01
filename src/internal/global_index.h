#ifndef KVLITE_INTERNAL_GLOBAL_INDEX_H
#define KVLITE_INTERNAL_GLOBAL_INDEX_H

#include <cstdint>
#include <functional>
#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

#include "internal/read_write_delta_hash_table.h"
#include "kvlite/status.h"

namespace kvlite {
namespace internal {

class GlobalIndexWAL;
class Manifest;

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
// 1. WAL (append-only delta log for crash recovery)
// 2. Periodic savepoints (full dump every N updates + on shutdown)
//
// Thread-safety: Index operations are thread-safe via per-bucket spinlocks
// in the underlying DeltaHashTable. Lifecycle methods (open/recover/close)
// must be called from a single thread.
//
// Savepoint locking: callers must hold a BatchGuard (shared lock on
// savepoint_mu_) for the duration of a WB flush or GC merge. This blocks
// storeSavepoint() (which takes an exclusive lock) but allows WB flush and
// GC to run concurrently. Reads don't acquire savepoint_mu_.
class GlobalIndex {
public:
    struct Options {
        // Sync WAL to disk on every write (slower but more durable)
        bool sync_writes = false;

        // Number of threads for parallel savepoint writes
        uint32_t savepoint_threads = 4;
    };

    // RAII guard that holds a shared lock on savepoint_mu_.
    // Callers must hold a BatchGuard for the entire WB flush or GC merge.
    // This allows concurrent WB/GC operations but blocks savepoints.
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

    Status open(const std::string& db_path, const Options& options);
    Status close();
    bool isOpen() const;

    // --- Index Operations ---

    // Single-insert convenience (self-locking). For batch operations use
    // BatchGuard + stagePut() + commitWB().
    Status put(uint64_t hkey, uint64_t packed_version, uint32_t segment_id);

    // Stage a put in the WAL buffer without auto-commit.
    // Caller must hold a BatchGuard and call commitWB() when done.
    Status stagePut(uint64_t hkey, uint64_t packed_version, uint32_t segment_id);

    bool get(uint64_t hkey,
             std::vector<uint32_t>& segment_ids,
             std::vector<uint64_t>& packed_versions) const;

    bool get(uint64_t hkey, uint64_t upper_bound,
             uint64_t& packed_version, uint32_t& segment_id) const;

    Status getLatest(uint64_t hkey,
                     uint64_t& packed_version, uint32_t& segment_id) const;

    bool contains(uint64_t hkey) const;

    // Update segment_id for an existing entry. Caller must hold a BatchGuard.
    Status relocate(uint64_t hkey, uint64_t packed_version,
                    uint32_t old_segment_id, uint32_t new_segment_id);

    // Remove an entry. Caller must hold a BatchGuard.
    Status eliminate(uint64_t hkey, uint64_t packed_version,
                     uint32_t segment_id);

    // --- Iteration ---

    void forEachGroup(
        const std::function<void(uint64_t hash,
                                 const std::vector<uint64_t>& packed_versions,
                                 const std::vector<uint32_t>& segment_ids)>& fn) const;

    void clear();

    // --- WAL commit (caller must hold a BatchGuard) ---

    Status commitWB(uint64_t max_version);
    Status commitGC();

    // --- Savepoint ---

    // Takes exclusive lock on savepoint_mu_, writes savepoint to disk,
    // persists max_version to Manifest, truncates WAL.
    Status storeSavepoint(uint64_t snapshot_version);

    // Create a savepoint if the WAL has accumulated any changes.
    Status maybeSavepoint();

    // --- Statistics ---

    size_t keyCount() const;
    size_t entryCount() const;
    size_t memoryUsage() const;

    // Fraction of key groups with more than one version (0.0–1.0).
    double estimateDeadRatio() const;

private:
    // --- Core DHT mutations (no WAL, no savepoint counter) ---
    // Used by both the public API (which also writes to WAL) and WAL replay.
    void applyPut(uint64_t hkey, uint64_t packed_version, uint32_t segment_id);
    void applyRelocate(uint64_t hkey, uint64_t packed_version,
                       uint32_t old_segment_id, uint32_t new_segment_id);
    void applyEliminate(uint64_t hkey, uint64_t packed_version,
                        uint32_t segment_id);

    Status recover();
    std::string savepointDir() const;

    // Savepoint format (binary, multi-file):
    // Directory at <db_path>/gi/savepoint/ with numbered .dat files.
    // Each file contains a contiguous range of main arena buckets.
    // The last file also contains all extension slot data.

    struct SavepointFileDesc {
        uint32_t file_index;
        uint32_t bucket_start;
        uint32_t bucket_count;
        bool is_last;
    };

    Status writeSavepoint(const std::string& dir) const;
    std::vector<SavepointFileDesc> computeFileLayout() const;
    Status writeSavepointFile(const std::string& dir,
                             const SavepointFileDesc& fd) const;

    Status readSavepoint(const std::string& dir);
    Status readSavepointFile(const std::string& fpath,
                            uint32_t stride,
                            uint64_t& out_entries,
                            uint64_t& out_key_count,
                            uint32_t& out_ext_count);

    // --- Data ---
    ReadWriteDeltaHashTable dht_;
    size_t key_count_ = 0;

    // --- Concurrency ---
    // Savepoint mutex: shared for writes (put/relocate/eliminate/commit),
    // exclusive for storeSavepoint(). Reads don't acquire this lock.
    mutable std::shared_mutex savepoint_mu_;

    // --- Lifecycle / persistence ---
    Manifest& manifest_;
    std::string db_path_;
    Options options_;
    bool is_open_ = false;
    std::unique_ptr<GlobalIndexWAL> wal_;
    uint64_t max_version_ = 0;
};

} // namespace internal
} // namespace kvlite

#endif // KVLITE_INTERNAL_GLOBAL_INDEX_H
