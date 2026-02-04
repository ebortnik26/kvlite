#ifndef KVLITE_INTERNAL_L1_INDEX_MANAGER_H
#define KVLITE_INTERNAL_L1_INDEX_MANAGER_H

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "kvlite/status.h"

namespace kvlite {
namespace internal {

// Forward declarations
class L1Index;
class L1WAL;

// L1 Index Manager: Manages the in-memory L1 index with persistence.
//
// Encapsulates:
// - L1Index: in-memory map (key → [(segment_id, version), ...])
// - L1WAL: write-ahead log for crash recovery
// - Snapshotting: periodic full dumps to reduce WAL replay time
//
// Recovery sequence:
// 1. Load latest snapshot (if exists)
// 2. Replay WAL entries on top of snapshot
// 3. Index is now current
//
// Persistence model:
// - Every put() is logged to WAL before returning
// - After N updates (configurable), a snapshot is taken
// - After successful snapshot, WAL is truncated
//
// Thread-safety: All public methods are thread-safe.
class L1IndexManager {
public:
    struct Options {
        // Number of updates before auto-snapshot (0 = disabled)
        uint64_t snapshot_interval = 10'000'000;

        // Sync WAL to disk on every write (slower but more durable)
        bool sync_writes = false;
    };

    L1IndexManager();
    ~L1IndexManager();

    // Non-copyable
    L1IndexManager(const L1IndexManager&) = delete;
    L1IndexManager& operator=(const L1IndexManager&) = delete;

    // --- Lifecycle ---

    // Open the index manager for a database path.
    // Does NOT recover - call recover() separately after open().
    Status open(const std::string& db_path, const Options& options);

    // Recover index state from snapshot + WAL.
    // Must be called after open() and before any read/write operations.
    // Returns OK even if no snapshot exists (starts with empty index).
    Status recover();

    // Close the index manager.
    // Takes a final snapshot if there are pending updates.
    Status close();

    // Check if open
    bool isOpen() const;

    // --- Index Operations ---

    // Insert a new entry for a key.
    // Logs to WAL, then updates in-memory index.
    // May trigger auto-snapshot if snapshot_interval is reached.
    Status put(const std::string& key, uint64_t version, uint32_t segment_id);

    // Get all (segment_id, version) pairs for a key.
    // Returns false if key doesn't exist.
    // Pairs are ordered latest-first (highest version first).
    bool get(const std::string& key,
             std::vector<uint32_t>& segment_ids,
             std::vector<uint64_t>& versions) const;

    // Get the latest entry for a key with version <= upper_bound.
    // Returns false if no matching entry exists.
    bool get(const std::string& key, uint64_t upper_bound,
             uint64_t& version, uint32_t& segment_id) const;

    // Get the latest (highest version) entry for a key.
    // Returns NotFound if key doesn't exist.
    Status getLatest(const std::string& key,
                     uint64_t& version, uint32_t& segment_id) const;

    // Check if a key exists (has any version)
    bool contains(const std::string& key) const;

    // Remove all entries for a key (used during GC).
    // Logs to WAL, then updates in-memory index.
    Status remove(const std::string& key);

    // Remove all entries pointing to a specific segment_id from a key's list.
    // Note: This is an in-memory-only operation, not logged to WAL.
    void removeSegment(const std::string& key, uint32_t segment_id);

    // --- Maintenance ---

    // Force a snapshot now.
    // Saves full index to snapshot file, then truncates WAL.
    Status snapshot();

    // Sync WAL to disk (if not using sync_writes option)
    Status sync();

    // --- Statistics ---

    size_t keyCount() const;
    size_t entryCount() const;
    size_t memoryUsage() const;
    uint64_t updatesSinceSnapshot() const;

private:
    Status maybeSnapshot();
    std::string snapshotPath() const;
    std::string walPath() const;

    std::string db_path_;
    Options options_;
    bool is_open_ = false;

    std::unique_ptr<L1Index> index_;
    std::unique_ptr<L1WAL> wal_;

    uint64_t updates_since_snapshot_ = 0;
    mutable std::mutex mutex_;
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_L1_INDEX_MANAGER_H
