#ifndef KVLITE_DB_H
#define KVLITE_DB_H

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "kvlite/status.h"
#include "kvlite/options.h"
#include "kvlite/batch.h"
#include "kvlite/iterator.h"

namespace kvlite {

// Forward declarations for internal components
namespace internal {
class GlobalIndex;
class Manifest;
class Segment;
class SegmentStorageManager;
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

class Snapshot {
public:
    uint64_t version() const { return version_; }

private:
    friend class DB;
    explicit Snapshot(uint64_t version) : version_(version) {}
    uint64_t version_;
};

class DB {
public:

    DB();
    ~DB();

    DB(const DB&) = delete;
    DB& operator=(const DB&) = delete;
    DB(DB&&) = delete;
    DB& operator=(DB&&) = delete;

    // --- Lifecycle ---

    Status open(const std::string& path, const Options& options = Options());
    Status close();
    bool isOpen() const;

    // --- Point Operations ---

    Status put(const std::string& key, const std::string& value,
               const WriteOptions& options = WriteOptions());

    Status get(const std::string& key, std::string& value,
               const ReadOptions& options = ReadOptions());

    Status get(const std::string& key, std::string& value,
               uint64_t& version,
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

    Snapshot createSnapshot();
    void releaseSnapshot(const Snapshot& snapshot);

    // --- Iteration ---

    Status createIterator(std::unique_ptr<Iterator>& iterator,
                          const ReadOptions& options = ReadOptions());

    // --- Statistics ---

    Status getStats(DBStats& stats) const;
    Status getPath(std::string& path) const;

    // --- Version Info ---

    uint64_t getLatestVersion() const;
    uint64_t getOldestVersion() const;

private:
    friend class Iterator;

    Status getByVersion(const std::string& key, uint64_t upper_bound,
                        std::string& value,
                        const ReadOptions& options = ReadOptions());

    Status getByVersion(const std::string& key, uint64_t upper_bound,
                        std::string& value, uint64_t& entry_version,
                        const ReadOptions& options = ReadOptions());

    // Shared WB + GI resolution for get/exists.
    // On WB hit: sets wb_hit=true, wb_tombstone, wb_value, wb_version.
    // On GI hit: sets wb_hit=false, returns segment pointer.
    // Returns NotFound if neither WB nor GI has the key.
    struct ResolveResult {
        bool wb_hit = false;
        bool wb_tombstone = false;
        std::string wb_value;
        uint64_t wb_version = 0;
        internal::Segment* segment = nullptr;
        uint64_t gi_packed_version = 0;  // packed version from GI (has tombstone in LSB)
    };
    Status resolve(const std::string& key, uint64_t upper_bound,
                   ResolveResult& result);

    Status flush();
    void cleanupRetiredBuffers();

    std::string db_path_;
    Options options_;
    std::unique_ptr<internal::Manifest> manifest_;
    std::unique_ptr<internal::VersionManager> versions_;
    std::unique_ptr<internal::GlobalIndex> global_index_;
    std::unique_ptr<internal::SegmentStorageManager> storage_;
    std::unique_ptr<internal::WriteBuffer> write_buffer_;
    std::vector<std::unique_ptr<internal::WriteBuffer>> retired_buffers_;
    uint32_t current_segment_id_ = 0;
    bool is_open_ = false;
    bool clean_close_persisted_ = true;
    std::mutex batch_mutex_;  // serializes batch write vs batch read snapshot
};

}  // namespace kvlite

#endif  // KVLITE_DB_H
