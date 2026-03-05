#ifndef KVLITE_DB_H
#define KVLITE_DB_H

#include <atomic>
#include <cstdint>
#include <memory>
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
class PeriodicDaemon;
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
    uint64_t current_version = 0;
    uint64_t oldest_version = 0;
    uint64_t active_snapshots = 0;

    // Background operation stats
    uint64_t flush_count = 0;
    uint64_t flush_total_us = 0;        // cumulative flush time in microseconds
    uint64_t gc_count = 0;
    uint64_t gc_total_us = 0;           // cumulative GC time in microseconds
    uint64_t savepoint_count = 0;
    uint64_t savepoint_total_us = 0;    // cumulative savepoint time in microseconds

    // Write stall stats (writer blocked waiting for flush to free a slot)
    uint64_t stall_count = 0;
    uint64_t stall_total_us = 0;        // cumulative stall time in microseconds

    // DHT codec instrumentation
    uint64_t dht_encode_count = 0;
    uint64_t dht_encode_total_ns = 0;
    uint64_t dht_decode_count = 0;
    uint64_t dht_decode_total_ns = 0;

    // DHT structure
    uint32_t dht_ext_count = 0;       // number of extension (overflow) buckets
    uint32_t dht_num_buckets = 0;     // number of primary buckets
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
        uint64_t hkey = 0;
    };
    Status resolve(const std::string& key, uint64_t upper_bound,
                   ResolveResult& result);

    Status flush();

    Status runGC();
    std::vector<uint32_t> selectGCInputs();
    Status mergeSegments(const std::vector<uint32_t>& input_ids);


    std::string db_path_;
    Options options_;
    std::unique_ptr<internal::Manifest> manifest_;
    std::unique_ptr<internal::VersionManager> versions_;
    std::unique_ptr<internal::GlobalIndex> global_index_;
    std::unique_ptr<internal::SegmentStorageManager> storage_;
    std::unique_ptr<internal::WriteBuffer> write_buffer_;
    bool is_open_ = false;

    std::unique_ptr<internal::PeriodicDaemon> gc_daemon_;
    std::unique_ptr<internal::PeriodicDaemon> sp_daemon_;

    // Background operation counters (updated from bg threads)
    std::atomic<uint64_t> flush_count_{0};
    std::atomic<uint64_t> flush_total_us_{0};
    std::atomic<uint64_t> gc_count_{0};
    std::atomic<uint64_t> gc_total_us_{0};
    std::atomic<uint64_t> savepoint_count_{0};
    std::atomic<uint64_t> savepoint_total_us_{0};
};

}  // namespace kvlite

#endif  // KVLITE_DB_H
