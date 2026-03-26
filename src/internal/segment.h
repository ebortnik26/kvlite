#ifndef KVLITE_INTERNAL_SEGMENT_H
#define KVLITE_INTERNAL_SEGMENT_H

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "internal/crc32.h"
#include "internal/segment_index.h"
#include "internal/log_entry.h"
#include "internal/log_file.h"

namespace kvlite {
namespace internal {

// Lineage entry types.
enum class LineageType : uint8_t {
    kFlush = 1,  // New-write segment (from memtable flush)
    kGC    = 2,  // GC output segment (compaction)
};

// A single lineage record for GlobalIndex recovery.
struct LineageEntry {
    uint64_t hkey;
    uint64_t packed_version;
    uint32_t old_segment_id;  // Only set for GC eliminations.
};

// Lineage section read from a sealed segment.
struct Lineage {
    LineageType type;
    std::vector<LineageEntry> entries;        // Put/relocated entries.
    std::vector<LineageEntry> eliminations;   // GC eliminations only.
};

// A Segment stores log entries partitioned across K files, each with its own
// SegmentIndex and lineage section.  K=1 is the single-file degenerate case.
//
// Per-partition file layout:
//   [LogEntry 0] ... [LogEntry N-1]  (data region)
//   [SegmentIndex: magic + entries + crc]
//   [Lineage: magic + type + entries + crc]
//   [segment_id:4][num_partitions:2][index_offset:8][lineage_offset:8][magic:4]
//
// File naming: segment_<id>_<partition>.data  (e.g. segment_7_0.data)
//
// Hash routing: partition = hash >> (64 - partition_bits).
// The Memtable iterates buckets in hash order, so consecutive hash ranges
// map to the same partition — writes to each file are sequential.
//
// State machine:
//   Closed  -> Writing   (create)
//   Closed  -> Readable  (open)
//   Writing -> Readable  (seal)
//   Writing -> Closed    (close)
//   Readable -> Closed   (close)
//
// Writing state:  put, appendEntry, appendRawEntry, seal, close
// Readable state: getLatest, get, contains, readLineage, close
class Segment {
public:
    Segment();
    ~Segment();

    Segment(const Segment&) = delete;
    Segment& operator=(const Segment&) = delete;
    Segment(Segment&&) noexcept;
    Segment& operator=(Segment&&) noexcept;

    enum class State { kClosed, kWriting, kReadable };

    // --- Lifecycle ---

    // Create K partition files for writing. Closed -> Writing.
    Status create(const std::string& path, uint32_t id,
                  uint16_t num_partitions = 1, bool buffered = true);

    // Open an existing segment (discovers K from first partition's footer).
    // Closed -> Readable.
    Status open(const std::string& path);

    // Seal all partitions: write SegmentIndex + lineage + footer + fdatasync
    // for each partition.  When K > 1 and a pool is provided, partitions are
    // sealed in parallel.  Writing -> Readable.
    Status seal(class FlushPool* pool = nullptr);

    // Close all partition files. Writing|Readable -> Closed.
    Status close();

    State state() const { return state_; }
    bool isOpen() const { return state_ != State::kClosed; }
    uint32_t getId() const { return id_; }
    uint16_t numPartitions() const { return num_partitions_; }

    // --- Lineage ---

    // Set the lineage type before writing entries.
    void setLineageType(LineageType type);

    // Record a GC elimination (entry removed, not relocated).
    void addLineageElimination(uint64_t hkey, uint64_t packed_version,
                               uint32_t old_segment_id);

    // Read lineage from all partitions of a sealed segment.
    Status readLineage(Lineage& lineage) const;

    // --- Write (Writing only) ---

    // Serialize a LogEntry, append to the partition selected by hash,
    // and update that partition's SegmentIndex.
    Status put(std::string_view key, uint64_t version,
               std::string_view value, bool tombstone,
               uint64_t hash);

    // Streaming batch append. Routes to partition by hash.
    Status appendEntry(std::string_view key, uint64_t version,
                       std::string_view value, bool tombstone,
                       uint64_t hash, uint64_t& entry_offset);

    // Zero-copy raw append. Routes to partition by hash.
    Status appendRawEntry(const void* payload, size_t payload_len,
                          uint64_t hash, uint64_t& entry_offset);

    // --- Read (Readable only) ---

    Status getLatest(uint64_t hash, LogEntry& entry) const;
    Status get(uint64_t hash, std::vector<LogEntry>& entries) const;
    Status get(uint64_t hash, uint64_t upper_bound, LogEntry& entry) const;
    bool contains(uint64_t hash) const;

    // --- Stats ---

    // Access partition 0's LogFile (for GC scan streams that read data region).
    // With K>1, use logFile(partition) instead.
    const LogFile& logFile() const { return partitions_[0].log_file; }
    const LogFile& logFile(uint16_t partition) const { return partitions_[partition].log_file; }

    const SegmentIndex& index() const { return partitions_[0].index; }
    const SegmentIndex& index(uint16_t partition) const { return partitions_[partition].index; }

    uint64_t dataSize() const;
    uint64_t dataSize(uint16_t partition) const;
    size_t keyCount() const;
    size_t entryCount() const;

    // Read and CRC-validate a LogEntry from a specific partition.
    Status readEntry(uint16_t partition, uint64_t offset, LogEntry& entry) const;

    // Codec instrumentation (summed across all partitions).
    uint64_t siEncodeCount() const;
    uint64_t siEncodeTotalNs() const;
    uint64_t siDecodeCount() const;
    uint64_t siDecodeTotalNs() const;

    // Generate the partition file path from a base path and partition index.
    static std::string partitionPath(const std::string& base_path, uint16_t partition);

private:
    // Per-partition state.
    struct Partition {
        LogFile log_file;
        SegmentIndex index;
        uint64_t data_size = 0;
        uint64_t lineage_offset = 0;
        bool batch_mode = false;

        // Lineage accumulation (Writing state only).
        std::vector<uint8_t> lineage_buf;
        uint32_t lineage_entry_count = 0;
        uint32_t lineage_elimination_count = 0;
    };

    uint16_t partitionFor(uint64_t hash) const;

    // Serialize and append a LogEntry to a partition, return file offset.
    Status writeEntry(Partition& p, std::string_view key, uint64_t version,
                      std::string_view value, bool tombstone,
                      uint64_t& entry_offset);

    // Add a lineage entry to a partition's buffer.
    static void addLineageEntry(Partition& p, uint64_t hkey, uint64_t packed_version);

    // Write one partition's lineage section.
    static Status writeLineage(Partition& p, LineageType type, uint32_t lineage_magic);

    // Seal a single partition (index + lineage + footer + fdatasync).
    Status sealPartition(uint16_t pi);

    // Read lineage from one partition file.
    Status readPartitionLineage(const Partition& p, Lineage& lineage) const;

    static constexpr uint32_t kFooterMagic = 0x53454746;  // "SEGF"
    static constexpr uint32_t kLineageMagic = 0x4C494E47; // "LING"
    // segment_id(4) + num_partitions(2) + index_offset(8) + lineage_offset(8) + magic(4)
    static constexpr size_t kFooterSize = 26;

    std::vector<Partition> partitions_;
    uint32_t id_ = 0;
    uint16_t num_partitions_ = 0;
    uint16_t partition_bits_ = 0;  // log2(num_partitions_)
    State state_ = State::kClosed;
    LineageType lineage_type_ = LineageType::kFlush;
    std::string base_path_;  // e.g. "/db/segments/segment_7"
};

// Thread pool for parallel partition sealing.  Owned by SegmentStorageManager.
class FlushPool {
public:
    explicit FlushPool(size_t num_threads);
    ~FlushPool();

    FlushPool(const FlushPool&) = delete;
    FlushPool& operator=(const FlushPool&) = delete;

    void submit(std::function<void()> fn);
    void wait();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_SEGMENT_H
