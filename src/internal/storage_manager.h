#ifndef KVLITE_INTERNAL_STORAGE_MANAGER_H
#define KVLITE_INTERNAL_STORAGE_MANAGER_H

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "kvlite/status.h"
#include "kvlite/options.h"

namespace kvlite {
namespace internal {

// Forward declarations
class WriteBuffer;
class DataCache;
class LogManager;
class SegmentIndex;
class SegmentIndexCache;
struct LogEntry;

// Storage Manager: Manages the data storage layer.
//
// Encapsulates:
// - WriteBuffer: pending writes before flush to log files
// - LogManager: append-only log files (immutable after write)
// - SegmentIndexCache: LRU cache of per-file SegmentIndex instances (key → offset)
// - DataCache: read cache for recently accessed values
// - GC: background garbage collection of old log files
//
// Write path:
//   writeEntry() → WriteBuffer → (on flush) → LogFile + SegmentIndex
//
// Read path:
//   readValue() → DataCache → SegmentIndexCache → LogFile
//
// GC runs in the background, compacting log files with high dead-entry ratios.
// GC respects the oldest_safe_version to avoid collecting data still needed
// by active snapshots.
//
// Thread-safety: All public methods are thread-safe.
class StorageManager {
public:
    struct Options {
        size_t log_file_size = 1ULL * 1024 * 1024 * 1024;  // 1GB
        size_t write_buffer_size = 64 * 1024 * 1024;       // 64MB
        size_t segment_index_cache_size = 256 * 1024 * 1024;          // 256MB
        size_t data_cache_size = 128 * 1024 * 1024;        // 128MB

        GCPolicy gc_policy = GCPolicy::HIGHEST_DEAD_RATIO;
        double gc_threshold = 0.5;
        int gc_max_files = 10;

        bool sync_writes = false;
        bool verify_checksums = true;
    };

    // Callback to get the oldest safe version for GC.
    // GC will not collect entries with version >= this value.
    using OldestVersionFn = std::function<uint64_t()>;

    StorageManager();
    ~StorageManager();

    // Non-copyable
    StorageManager(const StorageManager&) = delete;
    StorageManager& operator=(const StorageManager&) = delete;

    // --- Lifecycle ---

    // Open storage at the given path.
    // Does NOT recover - call recover() separately.
    Status open(const std::string& db_path, const Options& options,
                OldestVersionFn oldest_version_fn);

    // Recover storage state from disk.
    // Loads existing log files and rebuilds SegmentIndex instances.
    Status recover();

    // Close storage.
    // Flushes write buffer and stops GC thread.
    Status close();

    bool isOpen() const;

    // --- Write Operations ---

    // Write an entry to storage.
    // Returns the file_id where the entry was written (for GlobalIndex).
    // Entry goes to write buffer first; flushed to log on buffer full or flush().
    Status writeEntry(const std::string& key, const std::string& value,
                      uint64_t version, bool tombstone, uint32_t& file_id);

    // Flush write buffer to a new log file.
    Status flush();

    // Sync all pending writes to disk.
    Status sync();

    // --- Read Operations ---

    // Read a value from storage.
    // file_id comes from GlobalIndex, version is used for SegmentIndex lookup.
    Status readValue(uint32_t file_id, const std::string& key,
                     uint64_t version, std::string& value);

    // --- Iteration Support ---

    // Get all file IDs for iteration (sorted by file_id).
    std::vector<uint32_t> getFileIds() const;

    // Get SegmentIndex for a file (loads into cache if needed).
    SegmentIndex* getSegmentIndex(uint32_t file_id);

    // Read a log entry directly (for iterator).
    Status readLogEntry(uint32_t file_id, uint64_t offset, LogEntry& entry);

    // --- Statistics ---

    uint64_t numLogFiles() const;
    uint64_t totalLogSize() const;
    uint64_t segmentIndexCacheSize() const;
    uint64_t segmentIndexCachedCount() const;

private:
    // GC thread entry point
    void gcThreadMain();
    Status runGC();
    Status compactFile(uint32_t file_id, uint64_t oldest_safe_version);

    std::string db_path_;
    Options options_;
    OldestVersionFn oldest_version_fn_;
    bool is_open_ = false;

    // Storage components
    std::unique_ptr<WriteBuffer> write_buffer_;
    std::unique_ptr<DataCache> data_cache_;
    std::unique_ptr<LogManager> log_manager_;
    std::unique_ptr<SegmentIndexCache> segment_index_cache_;

    // GC state
    std::atomic<bool> gc_running_{false};
    std::atomic<bool> gc_stop_{false};
    std::thread gc_thread_;
    std::mutex gc_mutex_;
    std::condition_variable gc_cv_;
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_STORAGE_MANAGER_H
