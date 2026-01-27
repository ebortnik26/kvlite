#ifndef KVLITE_DB_H
#define KVLITE_DB_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "kvlite/status.h"
#include "kvlite/options.h"
#include "kvlite/write_batch.h"

namespace kvlite {

// Forward declaration
class DBImpl;

// Database statistics
struct DBStats {
    // Number of log files
    uint64_t num_log_files = 0;

    // Total size of all log files in bytes
    uint64_t total_log_size = 0;

    // Number of live entries (keys)
    uint64_t num_live_entries = 0;

    // Number of dead entries (tombstones + overwritten)
    uint64_t num_dead_entries = 0;

    // Size of L1 index in memory (bytes)
    uint64_t l1_index_size = 0;

    // Size of L2 cache in memory (bytes)
    uint64_t l2_cache_size = 0;

    // Number of L2 indices currently cached
    uint64_t l2_cached_count = 0;

    // Number of updates since last L1 snapshot
    uint64_t updates_since_snapshot = 0;

    // Whether GC is currently running
    bool gc_running = false;
};

// Information about a single log file
struct LogFileInfo {
    uint32_t file_id;
    uint64_t size;
    uint64_t live_entries;
    uint64_t dead_entries;
    double dead_ratio;  // dead_entries / (live + dead)
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
    void close();

    // Check if the database is open
    bool isOpen() const;

    // --- Basic Operations ---

    // Set the value for the given key
    // If the key exists, its value is overwritten
    Status put(const std::string& key, const std::string& value,
               const WriteOptions& options = WriteOptions());

    // Get the value for the given key
    // Returns Status::NotFound if the key does not exist
    Status get(const std::string& key, std::string* value,
               const ReadOptions& options = ReadOptions());

    // Remove the entry for the given key
    // Writes a tombstone; actual deletion happens during GC
    Status remove(const std::string& key,
                  const WriteOptions& options = WriteOptions());

    // Check if a key exists
    bool exists(const std::string& key,
                const ReadOptions& options = ReadOptions());

    // --- Batch Operations ---

    // Apply a batch of writes atomically
    // All operations in the batch succeed or fail together
    Status write(const WriteBatch& batch,
                 const WriteOptions& options = WriteOptions());

    // --- Garbage Collection ---

    // Trigger garbage collection manually
    // Compacts log files according to the configured GC policy
    // Returns immediately if GC is already running
    Status gc();

    // Trigger garbage collection on specific files
    // Merges the specified files, respecting 1GB output file limit
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
    DBStats getStats() const;

    // Get information about all log files
    std::vector<LogFileInfo> getLogFiles() const;

    // Get the database path
    const std::string& getPath() const;

private:
    std::unique_ptr<DBImpl> impl_;
};

} // namespace kvlite

#endif // KVLITE_DB_H
