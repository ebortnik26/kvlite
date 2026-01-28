#ifndef KVLITE_DB_ADMIN_H
#define KVLITE_DB_ADMIN_H

#include <cstdint>
#include <string>
#include <vector>

#include "kvlite/status.h"

namespace kvlite {

// Forward declaration
class DB;

// Database statistics
struct DBStats {
    // Number of log files
    uint64_t num_log_files = 0;

    // Total size of all log files in bytes
    uint64_t total_log_size = 0;

    // Number of live entries (latest version per key)
    uint64_t num_live_entries = 0;

    // Number of historical entries (older versions + tombstones)
    uint64_t num_historical_entries = 0;

    // Size of L1 index in memory (bytes)
    uint64_t l1_index_size = 0;

    // Size of L2 cache in memory (bytes)
    uint64_t l2_cache_size = 0;

    // Number of L2 indices currently cached
    uint64_t l2_cached_count = 0;

    // Number of updates since last L1 snapshot
    uint64_t updates_since_snapshot = 0;

    // Current global version (latest committed)
    uint64_t current_version = 0;

    // Oldest version still retained (limited by oldest snapshot)
    uint64_t oldest_version = 0;

    // Number of active snapshots
    uint64_t active_snapshots = 0;

    // Whether GC is currently running
    bool gc_running = false;
};

// Information about a single log file
struct LogFileInfo {
    uint32_t file_id;
    uint64_t size;
    uint64_t num_entries;
    uint64_t min_version;  // Oldest version in this file
    uint64_t max_version;  // Newest version in this file
    bool gc_eligible;      // True if all versions < oldest snapshot
};

// Administrative operations for DB
// These are separate from the core API to keep the main interface clean.
class DBAdmin {
public:
    explicit DBAdmin(DB& db);

    // --- Garbage Collection ---

    // Trigger garbage collection manually
    // Only collects versions older than the oldest active snapshot
    // Compacts log files according to the configured GC policy
    // Returns immediately if GC is already running
    Status gc();

    // Trigger garbage collection on specific files
    // Merges the specified files, respecting 1GB output file limit
    // Respects oldest snapshot version
    Status gc(const std::vector<uint32_t>& file_ids);

    // Check if GC is currently running
    bool isGCRunning() const;

    // Wait for any running GC to complete
    Status waitForGC();

    // --- Maintenance ---

    // Flush write buffer to disk
    // Creates a new log file with current buffer contents
    Status flush();

    // Force L1 index snapshot to disk
    // Truncates L1 WAL after successful snapshot
    Status snapshotL1Index();

    // --- Statistics ---

    // Get database statistics
    Status getStats(DBStats& stats) const;

    // Get information about all log files
    Status getLogFiles(std::vector<LogFileInfo>& files) const;

    // Get the database path
    Status getPath(std::string& path) const;

private:
    DB& db_;
};

} // namespace kvlite

#endif // KVLITE_DB_ADMIN_H
