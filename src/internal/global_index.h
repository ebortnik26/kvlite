#ifndef KVLITE_INTERNAL_L1_INDEX_H
#define KVLITE_INTERNAL_L1_INDEX_H

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
// 2. Periodic snapshots (full dump every N updates + on shutdown)
//
// Thread-safety: Index operations are thread-safe via per-bucket spinlocks
// in the underlying DeltaHashTable. Lifecycle methods (open/recover/close)
// must be called from a single thread.
//
// Snapshot locking: snapshot() takes an exclusive lock on snapshot_mu_ to
// block WB flush and GC writes. Normal reads are unaffected (per-bucket
// spinlocks suffice). Writers (put/relocate/eliminate) and commit methods
// take a shared lock on snapshot_mu_.
class GlobalIndex {
public:
    struct Options {
        // Number of updates before auto-snapshot (0 = disabled)
        uint64_t snapshot_interval = 10'000'000;

        // Sync WAL to disk on every write (slower but more durable)
        bool sync_writes = false;

        // Number of threads for parallel binary snapshot writes
        uint32_t snapshot_threads = 4;
    };

    explicit GlobalIndex(Manifest& manifest);
    ~GlobalIndex();

    // Non-copyable
    GlobalIndex(const GlobalIndex&) = delete;
    GlobalIndex& operator=(const GlobalIndex&) = delete;

    // --- Lifecycle ---

    Status open(const std::string& db_path, const Options& options);
    Status recover();
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

    // --- Binary snapshot ---

    // Takes exclusive lock on snapshot_mu_, records snapshot_version in Manifest,
    // writes binary v9 snapshot to disk, truncates WAL, resets update counter.
    // snapshot_version is the DB version at the time of snapshot (from VersionManager).
    Status storeSnapshot(uint64_t snapshot_version);

    // --- Statistics ---

    size_t keyCount() const;
    size_t entryCount() const;
    size_t memoryUsage() const;
    uint64_t updatesSinceSnapshot() const;

    // --- Persistence (low-level) ---

    Status saveSnapshot(const std::string& path) const;
    Status loadSnapshot(const std::string& path);

    // v7 snapshot file format (hash-based, single file):
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
    // v9 snapshot format (binary, multi-file):
    // Directory at <db_path>/gi/snapshot.v9/ with numbered files.
    // Each file contains a contiguous range of main arena buckets.
    // The last file also contains all extension slot data.
    // See saveBinarySnapshot() / loadBinarySnapshot() for details.

private:
    // --- Core DHT mutations (no WAL, no snapshot counter) ---
    // Used by both the public API (which also writes to WAL) and WAL replay.
    void applyPut(uint64_t hkey, uint64_t packed_version, uint32_t segment_id);
    void applyRelocate(uint64_t hkey, uint64_t packed_version,
                       uint32_t old_segment_id, uint32_t new_segment_id);
    void applyEliminate(uint64_t hkey, uint64_t packed_version,
                        uint32_t segment_id);

    Status maybeSnapshot();
    std::string snapshotPath() const;       // v7 single file path
    std::string snapshotDirV9() const;      // v9 directory path

    // v9 binary snapshot — write
    Status saveBinarySnapshot(const std::string& dir) const;

    struct SnapshotFileDesc {
        uint32_t file_index;
        uint32_t bucket_start;
        uint32_t bucket_count;
        bool is_last;
    };

    std::vector<SnapshotFileDesc> computeFileLayout() const;
    Status writeSnapshotFile(const std::string& dir,
                             const SnapshotFileDesc& fd) const;

    // v9 binary snapshot — read
    Status loadBinarySnapshot(const std::string& dir);
    Status loadSnapshotFile(const std::string& fpath,
                            uint32_t stride,
                            uint64_t& out_entries,
                            uint64_t& out_key_count,
                            uint32_t& out_ext_count);

    // v7 single-file snapshot — read
    Status loadV7Snapshot(const std::string& path);

    // --- Data ---
    ReadWriteDeltaHashTable dht_;
    size_t key_count_ = 0;

    // --- Concurrency ---
    // Snapshot mutex: shared for writes (put/relocate/eliminate/commit),
    // exclusive for snapshot(). Reads don't acquire this lock.
    mutable std::shared_mutex snapshot_mu_;

    // --- Lifecycle / persistence ---
    Manifest& manifest_;
    std::string db_path_;
    Options options_;
    bool is_open_ = false;
    std::unique_ptr<GlobalIndexWAL> wal_;
    uint64_t updates_since_snapshot_ = 0;
    uint64_t max_version_ = 0;
};

} // namespace internal
} // namespace kvlite

#endif // KVLITE_INTERNAL_L1_INDEX_H
