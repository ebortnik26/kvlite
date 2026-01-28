#ifndef KVLITE_DB_H
#define KVLITE_DB_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "kvlite/status.h"
#include "kvlite/options.h"
#include "kvlite/write_batch.h"
#include "kvlite/read_batch.h"

namespace kvlite {

// Forward declarations
class DBImpl;
class Snapshot;

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

class DB {
public:
    DB();
    ~DB();

    // Non-copyable
    DB(const DB&) = delete;
    DB& operator=(const DB&) = delete;

    // Movable
    DB(DB&& other) noexcept;
    DB& operator=(DB&& other) noexcept;

    // --- Lifecycle ---

    // Open a database at the specified path
    // Creates the directory if options.create_if_missing is true
    Status open(const std::string& path, const Options& options = Options());

    // Close the database
    // Flushes write buffer, persists L1 snapshot, and releases resources
    Status close();

    // Check if the database is open
    bool isOpen() const;

    // --- Basic Operations ---

    // Set the value for the given key
    // Creates a new version; previous versions are retained until GC
    Status put(const std::string& key, const std::string& value,
               const WriteOptions& options = WriteOptions());

    // Get the latest value for the given key
    // Returns Status::NotFound if the key does not exist
    Status get(const std::string& key, std::string& value,
               const ReadOptions& options = ReadOptions());

    // Get the latest value and its version
    Status get(const std::string& key, std::string& value, uint64_t& version,
               const ReadOptions& options = ReadOptions());

    // Get the value at a specific version (point-in-time read)
    // Returns the latest version of key where version < upper_bound
    // Returns Status::NotFound if key did not exist at that version
    Status getByVersion(const std::string& key, uint64_t upper_bound,
                        std::string& value,
                        const ReadOptions& options = ReadOptions());

    // Get the value at a specific version with version info
    Status getByVersion(const std::string& key, uint64_t upper_bound,
                        std::string& value, uint64_t& entry_version,
                        const ReadOptions& options = ReadOptions());

    // Remove the entry for the given key
    // Writes a tombstone with a new version; actual deletion happens during GC
    Status remove(const std::string& key,
                  const WriteOptions& options = WriteOptions());

    // Check if a key exists (latest version)
    Status exists(const std::string& key, bool& exists,
                  const ReadOptions& options = ReadOptions());

    // --- Batch Operations ---

    // Apply a batch of writes atomically
    // All operations in the batch get the same version
    Status write(const WriteBatch& batch,
                 const WriteOptions& options = WriteOptions());

    // Execute a batch of reads at a consistent snapshot
    // Creates a temporary snapshot, performs all reads, then releases it
    // Results are stored in the batch object
    Status read(ReadBatch& batch,
                const ReadOptions& options = ReadOptions());

    // --- Snapshots ---

    // Create a snapshot at the current version
    // The snapshot provides a consistent point-in-time view
    // Must be released with releaseSnapshot() to allow GC
    Status createSnapshot(std::unique_ptr<Snapshot>& snapshot);

    // Release a snapshot, allowing GC of versions it was protecting
    Status releaseSnapshot(std::unique_ptr<Snapshot> snapshot);

    // Get the current (latest committed) version
    Status getCurrentVersion(uint64_t& version) const;

    // Get the oldest version still retained (limited by snapshots)
    Status getOldestVersion(uint64_t& version) const;

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
    const std::string& getPath() const;

private:
    std::unique_ptr<DBImpl> impl_;
};

} // namespace kvlite

#endif // KVLITE_DB_H
