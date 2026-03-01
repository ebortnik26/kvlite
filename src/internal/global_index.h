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
// Savepoint locking: storeSavepoint() takes an exclusive lock on savepoint_mu_
// to block WB flush and GC writes. Normal reads are unaffected (per-bucket
// spinlocks suffice). Writers (put/relocate/eliminate) and commit methods
// take a shared lock on savepoint_mu_.
class GlobalIndex {
public:
    struct Options {
        // Number of updates before auto-savepoint (0 = disabled)
        uint64_t savepoint_interval = 1ULL << 24;

        // Sync WAL to disk on every write (slower but more durable)
        bool sync_writes = false;

        // Number of threads for parallel binary savepoint writes
        uint32_t savepoint_threads = 4;
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

    Status put(uint64_t hkey, uint64_t packed_version, uint32_t segment_id);

    bool get(uint64_t hkey,
             std::vector<uint32_t>& segment_ids,
             std::vector<uint64_t>& packed_versions) const;

    bool get(uint64_t hkey, uint64_t upper_bound,
             uint64_t& packed_version, uint32_t& segment_id) const;

    Status getLatest(uint64_t hkey,
                     uint64_t& packed_version, uint32_t& segment_id) const;

    bool contains(uint64_t hkey) const;

    // Update segment_id for an existing entry.
    Status relocate(uint64_t hkey, uint64_t packed_version,
                    uint32_t old_segment_id, uint32_t new_segment_id);

    // Remove an entry. If the key's fingerprint group becomes empty, decrements key_count_.
    Status eliminate(uint64_t hkey, uint64_t packed_version,
                     uint32_t segment_id);

    // --- Iteration ---

    void forEachGroup(
        const std::function<void(uint64_t hash,
                                 const std::vector<uint64_t>& packed_versions,
                                 const std::vector<uint32_t>& segment_ids)>& fn) const;

    void clear();

    // --- WAL commit (hides producer IDs from callers) ---

    Status commitWB(uint64_t max_version);
    Status commitGC();

    // --- Savepoint ---

    // Takes exclusive lock on savepoint_mu_, writes binary v9 savepoint to
    // disk, persists max_version to Manifest, truncates WAL, resets counter.
    Status storeSavepoint(uint64_t snapshot_version);

    // --- Statistics ---

    size_t keyCount() const;
    size_t entryCount() const;
    size_t memoryUsage() const;
    uint64_t updatesSinceSavepoint() const;

    // --- Persistence (low-level) ---

    Status saveSavepoint(const std::string& path) const;
    Status loadSavepoint(const std::string& path);

    // v7 savepoint file format (hash-based, single file):
    // [magic: 4 bytes]["L1IX"]
    // [format_version: 4 bytes][7]
    // [num_entries: 8 bytes]
    // [key_count: 8 bytes]
    // Per entry (via forEach):
    //   [hash: 8 bytes]
    //   [packed_version: 8 bytes]
    //   [segment_id: 4 bytes]
    // [checksum: 4 bytes]
    //
    // v9 savepoint format (binary, multi-file):
    // Directory at <db_path>/gi/snapshot.v9/ with numbered files.
    // Each file contains a contiguous range of main arena buckets.
    // The last file also contains all extension slot data.
    // See saveBinarySavepoint() / loadBinarySavepoint() for details.

private:
    // --- Core DHT mutations (no WAL, no savepoint counter) ---
    // Used by both the public API (which also writes to WAL) and WAL replay.
    void applyPut(uint64_t hkey, uint64_t packed_version, uint32_t segment_id);
    void applyRelocate(uint64_t hkey, uint64_t packed_version,
                       uint32_t old_segment_id, uint32_t new_segment_id);
    void applyEliminate(uint64_t hkey, uint64_t packed_version,
                        uint32_t segment_id);

    Status recover();
    Status maybeSavepoint();
    std::string savepointPath() const;       // v7 single file path
    std::string savepointDirV9() const;      // v9 directory path

    // v9 binary savepoint — write
    Status saveBinarySavepoint(const std::string& dir) const;

    struct SavepointFileDesc {
        uint32_t file_index;
        uint32_t bucket_start;
        uint32_t bucket_count;
        bool is_last;
    };

    std::vector<SavepointFileDesc> computeFileLayout() const;
    Status writeSavepointFile(const std::string& dir,
                             const SavepointFileDesc& fd) const;

    // v9 binary savepoint — read
    Status loadBinarySavepoint(const std::string& dir);
    Status loadSavepointFile(const std::string& fpath,
                            uint32_t stride,
                            uint64_t& out_entries,
                            uint64_t& out_key_count,
                            uint32_t& out_ext_count);

    // v7 single-file savepoint — read
    Status loadV7Savepoint(const std::string& path);

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
    uint64_t updates_since_savepoint_ = 0;
    uint64_t max_version_ = 0;
};

} // namespace internal
} // namespace kvlite

#endif // KVLITE_INTERNAL_GLOBAL_INDEX_H
