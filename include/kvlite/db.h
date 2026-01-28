#ifndef KVLITE_DB_H
#define KVLITE_DB_H

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "kvlite/status.h"
#include "kvlite/options.h"
#include "kvlite/write_batch.h"
#include "kvlite/read_batch.h"

namespace kvlite {

// Forward declarations for internal components
namespace internal {
class WriteBuffer;
class DataCache;
class LogManager;
class L1IndexManager;
class L2Index;
class L2Cache;
struct LogEntry;
struct IndexEntry;
}  // namespace internal

// Database statistics
struct DBStats {
    uint64_t num_log_files = 0;
    uint64_t total_log_size = 0;
    uint64_t num_live_entries = 0;
    uint64_t num_historical_entries = 0;
    uint64_t l1_index_size = 0;
    uint64_t l2_cache_size = 0;
    uint64_t l2_cached_count = 0;
    uint64_t updates_since_snapshot = 0;
    uint64_t current_version = 0;
    uint64_t oldest_version = 0;
    uint64_t active_snapshots = 0;
    bool gc_running = false;
};

// Information about a single log file
struct LogFileInfo {
    uint32_t file_id;
    uint64_t size;
    uint64_t num_entries;
    uint64_t min_version;
    uint64_t max_version;
    bool gc_eligible;
};

class DB {
public:
    // --- Nested Classes ---

    class Snapshot {
    public:
        ~Snapshot();

        Snapshot(const Snapshot&) = delete;
        Snapshot& operator=(const Snapshot&) = delete;

        Snapshot(Snapshot&& other) noexcept;
        Snapshot& operator=(Snapshot&& other) noexcept;

        Status get(const std::string& key, std::string& value,
                   const ReadOptions& options = ReadOptions()) const;

        Status get(const std::string& key, std::string& value, uint64_t& entry_version,
                   const ReadOptions& options = ReadOptions()) const;

        Status exists(const std::string& key, bool& exists,
                      const ReadOptions& options = ReadOptions()) const;

        uint64_t version() const;
        bool isValid() const;

    private:
        friend class DB;
        Snapshot(DB* db, uint64_t version);
        void detach();

        DB* db_;
        uint64_t version_;
    };

    class Iterator {
    public:
        ~Iterator();

        Iterator(const Iterator&) = delete;
        Iterator& operator=(const Iterator&) = delete;

        Iterator(Iterator&& other) noexcept;
        Iterator& operator=(Iterator&& other) noexcept;

        Status next(std::string& key, std::string& value);
        Status next(std::string& key, std::string& value, uint64_t& version);

        uint64_t snapshotVersion() const;

    private:
        friend class DB;
        class Impl;
        explicit Iterator(std::unique_ptr<Impl> impl);
        std::unique_ptr<Impl> impl_;
    };

    // --- DB Methods ---

    DB();
    ~DB();

    DB(const DB&) = delete;
    DB& operator=(const DB&) = delete;

    DB(DB&& other) noexcept;
    DB& operator=(DB&& other) noexcept;

    // --- Lifecycle ---

    Status open(const std::string& path, const Options& options = Options());
    Status close();
    bool isOpen() const;

    // --- Point Operations ---

    Status put(const std::string& key, const std::string& value,
               const WriteOptions& options = WriteOptions());

    Status get(const std::string& key, std::string& value,
               const ReadOptions& options = ReadOptions());

    Status get(const std::string& key, std::string& value, uint64_t& version,
               const ReadOptions& options = ReadOptions());

    Status getByVersion(const std::string& key, uint64_t upper_bound,
                        std::string& value,
                        const ReadOptions& options = ReadOptions());

    Status getByVersion(const std::string& key, uint64_t upper_bound,
                        std::string& value, uint64_t& entry_version,
                        const ReadOptions& options = ReadOptions());

    Status remove(const std::string& key,
                  const WriteOptions& options = WriteOptions());

    Status exists(const std::string& key, bool& exists,
                  const ReadOptions& options = ReadOptions());

    // --- Batch Operations ---

    Status write(const WriteBatch& batch,
                 const WriteOptions& options = WriteOptions());

    Status read(ReadBatch& batch,
                const ReadOptions& options = ReadOptions());

    // --- Snapshots ---

    Status createSnapshot(std::unique_ptr<Snapshot>& snapshot);
    Status releaseSnapshot(std::unique_ptr<Snapshot> snapshot);

    // --- Iteration ---

    Status createIterator(std::unique_ptr<Iterator>& iterator);

    // --- Garbage Collection ---

    Status gc();
    Status gc(const std::vector<uint32_t>& file_ids);
    bool isGCRunning() const;
    Status waitForGC();

    // --- Maintenance ---

    Status flush();
    Status snapshotL1Index();

    // --- Statistics ---

    Status getStats(DBStats& stats) const;
    Status getLogFiles(std::vector<LogFileInfo>& files) const;
    Status getPath(std::string& path) const;

    // --- Version Info ---

    uint64_t getCurrentVersion() const;
    uint64_t getOldestVersion() const;

private:
    // Internal snapshot management
    Status createSnapshotInternal(uint64_t& snapshot_version);
    Status releaseSnapshotInternal(uint64_t snapshot_version);
    uint64_t getOldestSnapshotVersion() const;

    // Internal helpers
    Status recover();
    Status writeEntry(const std::string& key, const std::string& value,
                      bool tombstone, const WriteOptions& options,
                      uint64_t& version);
    Status readValue(const std::string& key, uint64_t upper_bound,
                     std::string& value, uint64_t& version);

    uint64_t allocateVersion();
    void updateL1Index(const std::string& key, uint64_t version,
                       uint32_t file_id);

    Status syncIfNeeded(const WriteOptions& options);

    // GC helpers
    Status gcFile(uint32_t file_id, uint64_t min_version);
    bool canCollectVersion(uint64_t version) const;

    // Iterator support
    std::vector<uint32_t> getFileIdsForIteration() const;
    Status readLogEntry(uint32_t file_id, uint64_t offset,
                        internal::LogEntry& entry);
    bool isLatestVersion(const std::string& key, uint64_t version) const;
    internal::L2Index* getL2Index(uint32_t file_id);

    // State
    bool is_open_ = false;
    std::string db_path_;
    Options options_;

    // Core components
    std::unique_ptr<internal::WriteBuffer> write_buffer_;
    std::unique_ptr<internal::DataCache> data_cache_;
    std::unique_ptr<internal::LogManager> log_manager_;
    std::unique_ptr<internal::L1IndexManager> l1_index_;
    std::unique_ptr<internal::L2Cache> l2_cache_;

    // Version management
    std::atomic<uint64_t> current_version_{0};
    std::atomic<uint64_t> oldest_version_{0};

    // Snapshot tracking
    std::set<uint64_t> active_snapshots_;
    mutable std::mutex snapshot_mutex_;

    // GC state
    std::atomic<bool> gc_running_{false};
    std::thread gc_thread_;
    std::mutex gc_mutex_;
    std::condition_variable gc_cv_;

    // Write serialization
    mutable std::mutex write_mutex_;

    // General state mutex
    mutable std::shared_mutex state_mutex_;
};

}  // namespace kvlite

#endif  // KVLITE_DB_H
