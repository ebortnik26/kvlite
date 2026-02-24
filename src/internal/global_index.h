#ifndef KVLITE_INTERNAL_L1_INDEX_H
#define KVLITE_INTERNAL_L1_INDEX_H

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "internal/segment_delta_hash_table.h"
#include "kvlite/status.h"

namespace kvlite {
namespace internal {

class GlobalIndexWAL;

// GlobalIndex: In-memory index mapping keys to (segment_id, packed_version) lists,
// with built-in concurrency control and persistence.
//
// Structure: key → [(segment_id₁, packed_version₁), (segment_id₂, packed_version₂), ...]
//            sorted by packed_version desc (latest/highest first)
//
// Packed versions encode (logical_version << 1) | tombstone_bit.
//
// Backed by SegmentDeltaHashTable with swapped field mapping:
//   DHT "offsets" → packed_versions (sorted desc, so findFirst returns latest)
//   DHT "versions" → segment_ids (parallel)
//
// The GlobalIndex is always fully loaded in memory. It is persisted via:
// 1. WAL (append-only delta log for crash recovery)
// 2. Periodic snapshots (full dump every N updates + on shutdown)
//
// Thread-safety: Index operations are thread-safe via per-bucket spinlocks
// in the underlying DeltaHashTable. Lifecycle methods (open/recover/close)
// must be called from a single thread.
class GlobalIndex {
public:
    struct Options {
        // Number of updates before auto-snapshot (0 = disabled)
        uint64_t snapshot_interval = 10'000'000;

        // Sync WAL to disk on every write (slower but more durable)
        bool sync_writes = false;
    };

    GlobalIndex();
    ~GlobalIndex();

    // Non-copyable
    GlobalIndex(const GlobalIndex&) = delete;
    GlobalIndex& operator=(const GlobalIndex&) = delete;

    // --- Lifecycle ---

    // Open the index for a database path.
    // Does NOT recover — call recover() separately after open().
    Status open(const std::string& db_path, const Options& options);

    // Recover index state from snapshot + WAL.
    // Must be called after open() and before any read/write operations.
    // Returns OK even if no snapshot exists (starts with empty index).
    Status recover();

    // Close the index.
    // Takes a final snapshot if there are pending updates.
    Status close();

    // Check if open.
    bool isOpen() const;

    // --- Index Operations ---

    // Append (segment_id, packed_version) to key's list.
    // Logs to WAL, then updates in-memory index.
    // May trigger auto-snapshot if snapshot_interval is reached.
    Status put(const std::string& key, uint64_t packed_version, uint32_t segment_id);

    // Get all (segment_id, packed_version) pairs for a key. Returns false if key
    // doesn't exist. Pairs are ordered latest-first (highest packed_version first).
    bool get(const std::string& key,
             std::vector<uint32_t>& segment_ids,
             std::vector<uint64_t>& packed_versions) const;

    // Get the latest entry for a key with packed_version <= upper_bound.
    // Returns false if no matching entry exists.
    bool get(const std::string& key, uint64_t upper_bound,
             uint64_t& packed_version, uint32_t& segment_id) const;

    // Get the latest (highest packed_version) entry for a key.
    // Returns NotFound if key doesn't exist.
    Status getLatest(const std::string& key,
                     uint64_t& packed_version, uint32_t& segment_id) const;

    // Check if a key exists (has any version).
    bool contains(const std::string& key) const;

    // --- Iteration ---

    // Iterate over all groups (hash-sorted). Callback receives:
    //   hash: the FNV-1a hash
    //   packed_versions: sorted desc (latest first, mapped from DHT "offsets")
    //   segment_ids: parallel array (mapped from DHT "versions")
    void forEachGroup(
        const std::function<void(uint64_t hash,
                                 const std::vector<uint32_t>& packed_versions,
                                 const std::vector<uint32_t>& segment_ids)>& fn) const;

    // Clear all entries.
    void clear();

    // --- Maintenance ---

    // Force a snapshot now.
    // Saves full index to snapshot file, then truncates WAL.
    Status snapshot();

    // Sync WAL to disk (if not using sync_writes option).
    Status sync();

    // --- Statistics ---

    size_t keyCount() const;
    size_t entryCount() const;  // total (segment_id, version) refs across all keys
    size_t memoryUsage() const;
    uint64_t updatesSinceSnapshot() const;

    // --- Persistence (low-level) ---

    // Save full snapshot to file.
    Status saveSnapshot(const std::string& path) const;

    // Load snapshot from file.
    Status loadSnapshot(const std::string& path);

    // Snapshot file format (v6, hash-based):
    // [magic: 4 bytes]["L1IX" (legacy)]
    // [format_version: 4 bytes][6]
    // [num_entries: 8 bytes]
    // [key_count: 8 bytes]
    // Per entry (via forEach):
    //   [hash: 8 bytes]
    //   [packed_version: 4 bytes]
    //   [segment_id: 4 bytes]
    // [checksum: 4 bytes]

private:
    Status maybeSnapshot();
    std::string snapshotPath() const;
    std::string walPath() const;

    // --- Data ---
    SegmentDeltaHashTable dht_;
    size_t key_count_ = 0;

    // --- Lifecycle / persistence ---
    std::string db_path_;
    Options options_;
    bool is_open_ = false;
    std::unique_ptr<GlobalIndexWAL> wal_;
    uint64_t updates_since_snapshot_ = 0;
};

} // namespace internal
} // namespace kvlite

#endif // KVLITE_INTERNAL_L1_INDEX_H
