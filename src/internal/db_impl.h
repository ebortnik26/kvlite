#ifndef KVLITE_INTERNAL_DB_IMPL_H
#define KVLITE_INTERNAL_DB_IMPL_H

#include <cstdint>
#include <string>
#include <memory>
#include <atomic>
#include <mutex>
#include <shared_mutex>
#include <set>
#include <thread>
#include <condition_variable>

#include "kvlite/status.h"
#include "kvlite/options.h"
#include "kvlite/write_batch.h"
#include "kvlite/read_batch.h"
#include "kvlite/db_admin.h"

#include "log_file.h"
#include "l1_index.h"
#include "l2_index.h"
#include "l2_cache.h"
#include "l1_wal.h"
#include "data_cache.h"
#include "write_buffer.h"

namespace kvlite {
namespace internal {

// Forward declarations
class SnapshotImpl;
class IteratorImpl;

// Internal implementation of the DB class.
//
// Storage model:
// - Conceptually append-only: new versions are appended, old data is never modified
// - Physically write-once: log files are immutable after write, deleted only by GC
//
// Components:
// - WriteBuffer: pending writes before flush
// - DataCache: read cache for (key, version) -> value
// - LogManager: manages write-once log files
// - L1Index: in-memory index (key -> file_id)
// - L2Cache: LRU cache of per-file indices (key -> offset)
// - L1WAL: write-ahead log for L1 index recovery
//
// Write path: WriteBuffer -> (on flush) -> LogFile + L1Index
// Read path:  WriteBuffer -> DataCache -> L1Index -> L2Cache -> LogFile
//
// Thread-safety: All public methods are thread-safe.
class DBImpl {
public:
    DBImpl();
    ~DBImpl();

    // Non-copyable
    DBImpl(const DBImpl&) = delete;
    DBImpl& operator=(const DBImpl&) = delete;

    // --- Core Operations ---

    Status open(const std::string& path, const Options& options);
    Status close();
    bool isOpen() const;

    // Single key operations
    Status put(const std::string& key, const std::string& value,
               const WriteOptions& options);
    Status get(const std::string& key, std::string& value,
               const ReadOptions& options);
    Status get(const std::string& key, std::string& value, uint64_t& version,
               const ReadOptions& options);
    Status getByVersion(const std::string& key, uint64_t upper_bound,
                        std::string& value, const ReadOptions& options);
    Status remove(const std::string& key, const WriteOptions& options);
    Status exists(const std::string& key, bool& exists,
                  const ReadOptions& options);

    // Batch operations
    Status write(const WriteBatch& batch, const WriteOptions& options);
    Status read(ReadBatch& batch, const ReadOptions& options);

    // --- Snapshot Management ---

    Status createSnapshot(uint64_t& snapshot_version);
    Status releaseSnapshot(uint64_t snapshot_version);
    uint64_t getOldestSnapshotVersion() const;

    // --- Version Management ---

    uint64_t getCurrentVersion() const { return current_version_.load(); }
    uint64_t getOldestVersion() const { return oldest_version_.load(); }

    // --- Admin Operations ---

    Status gc();
    Status gc(const std::vector<uint32_t>& file_ids);
    bool isGCRunning() const;
    Status waitForGC();

    Status flush();
    Status snapshotL1Index();

    Status getStats(DBStats& stats) const;
    Status getLogFiles(std::vector<LogFileInfo>& files) const;
    Status getPath(std::string& path) const;

    // --- Iterator Support ---

    // Get all file IDs for iteration
    std::vector<uint32_t> getFileIdsForIteration() const;

    // Read entry from log file (for iterator)
    Status readLogEntry(uint32_t file_id, uint64_t offset, LogEntry& entry);

    // Check if a version is the latest for a key (for iterator filtering)
    bool isLatestVersion(const std::string& key, uint64_t version) const;

    // Get L2 index for a file (for iterator)
    L2Index* getL2Index(uint32_t file_id);

private:
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

    Status maybeSnapshotL1();
    Status syncIfNeeded(const WriteOptions& options);

    // GC helpers
    Status gcFile(uint32_t file_id, uint64_t min_version);
    bool canCollectVersion(uint64_t version) const;

    // State
    bool is_open_ = false;
    std::string db_path_;
    Options options_;

    // Core components
    std::unique_ptr<WriteBuffer> write_buffer_;  // pending writes
    std::unique_ptr<DataCache> data_cache_;      // (key, version) -> value
    std::unique_ptr<LogManager> log_manager_;
    std::unique_ptr<L1Index> l1_index_;
    std::unique_ptr<L2Cache> l2_cache_;          // file_id -> L2Index
    std::unique_ptr<L1WAL> l1_wal_;

    // Version management
    std::atomic<uint64_t> current_version_{0};
    std::atomic<uint64_t> oldest_version_{0};

    // Snapshot tracking (sorted set of active snapshot versions)
    std::set<uint64_t> active_snapshots_;
    mutable std::mutex snapshot_mutex_;

    // L1 snapshot management
    uint64_t l1_updates_since_snapshot_ = 0;
    static constexpr uint64_t kL1SnapshotInterval = 10'000'000;  // 10M updates

    // GC state
    std::atomic<bool> gc_running_{false};
    std::thread gc_thread_;
    std::mutex gc_mutex_;
    std::condition_variable gc_cv_;

    // Write serialization (for version allocation)
    mutable std::mutex write_mutex_;

    // General mutex for state changes (open/close)
    mutable std::shared_mutex state_mutex_;
};

// Snapshot implementation
class SnapshotImpl {
public:
    explicit SnapshotImpl(DBImpl* db, uint64_t version)
        : db_(db), version_(version) {}

    ~SnapshotImpl() {
        if (db_) {
            db_->releaseSnapshot(version_);
        }
    }

    uint64_t version() const { return version_; }

    Status get(const std::string& key, std::string& value) {
        return db_->getByVersion(key, version_ + 1, value, ReadOptions());
    }

    Status exists(const std::string& key, bool& exists) {
        std::string value;
        Status s = get(key, value);
        if (s.ok()) {
            exists = true;
            return Status::ok();
        } else if (s.isNotFound()) {
            exists = false;
            return Status::ok();
        }
        return s;
    }

private:
    DBImpl* db_;
    uint64_t version_;
};

// Iterator implementation
class IteratorImpl {
public:
    explicit IteratorImpl(DBImpl* db);
    ~IteratorImpl();

    bool valid() const;
    Status status() const;
    void next();

    const std::string& key() const;
    const std::string& value() const;
    uint64_t version() const;

private:
    Status advance();
    Status loadNextFile();
    Status loadNextEntry();

    DBImpl* db_;
    Status status_;

    // Current position
    std::vector<uint32_t> file_ids_;
    size_t current_file_idx_ = 0;
    L2Index* current_l2_ = nullptr;

    // Iteration state within current L2 index
    std::unordered_map<std::string, std::vector<IndexEntry>>::const_iterator key_iter_;
    std::unordered_map<std::string, std::vector<IndexEntry>>::const_iterator key_end_;
    size_t entry_idx_ = 0;

    // Current entry data
    bool valid_ = false;
    std::string current_key_;
    std::string current_value_;
    uint64_t current_version_ = 0;

    // Track keys we've already returned (to handle duplicates across files)
    std::unordered_set<std::string> seen_keys_;
};

} // namespace internal
} // namespace kvlite

#endif // KVLITE_INTERNAL_DB_IMPL_H
