#ifndef KVLITE_DB_H
#define KVLITE_DB_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "kvlite/status.h"
#include "kvlite/options.h"
#include "kvlite/batch.h"

namespace kvlite {

// Forward declarations for internal components
namespace internal {
class GlobalIndexManager;
class StorageManager;
class VersionManager;
class WriteBuffer;
}  // namespace internal

// Database statistics
struct DBStats {
    uint64_t num_log_files = 0;
    uint64_t total_log_size = 0;
    uint64_t num_live_entries = 0;
    uint64_t num_historical_entries = 0;
    uint64_t global_index_size = 0;
    uint64_t segment_index_cache_size = 0;
    uint64_t segment_index_cached_count = 0;
    uint64_t current_version = 0;
    uint64_t oldest_version = 0;
    uint64_t active_snapshots = 0;
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

        const Snapshot& snapshot() const;

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
    Status createIterator(const Snapshot& snapshot, std::unique_ptr<Iterator>& iterator);

    // --- Maintenance ---

    Status flush();

    // --- Statistics ---

    Status getStats(DBStats& stats) const;
    Status getPath(std::string& path) const;

    // --- Version Info ---

    uint64_t getLatestVersion() const;
    uint64_t getOldestVersion() const;

private:
    std::string db_path_;
    Options options_;
    std::unique_ptr<internal::VersionManager> versions_;
    std::unique_ptr<internal::GlobalIndexManager> global_index_;
    std::unique_ptr<internal::StorageManager> storage_;
    std::unique_ptr<internal::WriteBuffer> write_buffer_;
    uint32_t current_segment_id_ = 0;
};

}  // namespace kvlite

#endif  // KVLITE_DB_H
