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

// Lineage section read from a sealed segment.
//
// Each record is 20 bytes: {hkey, packed_version, old_segment_id}.
// For flush puts, old_segment_id is 0.  For GC relocations, it
// identifies the source segment.  Recovery disambiguates:
//   present + old_segment_id == 0  →  applyPut
//   present + old_segment_id != 0  →  applyRelocate
//   deleted                        →  applyEliminate
struct Lineage {
    struct Record {
        uint64_t hkey;
        uint64_t packed_version;
        uint32_t old_segment_id;  // 0 for flush puts
    };

    LineageType type;
    std::vector<Record> present;  // puts (flush) or relocations (GC)
    std::vector<Record> deleted;  // eliminations (GC only)
};

// ---------------------------------------------------------------------------
// SegmentPartition: one physical file with its own data, index, and lineage.
// ---------------------------------------------------------------------------

class SegmentPartition {
public:
    SegmentPartition();
    ~SegmentPartition();

    SegmentPartition(const SegmentPartition&) = delete;
    SegmentPartition& operator=(const SegmentPartition&) = delete;
    SegmentPartition(SegmentPartition&&) noexcept;
    SegmentPartition& operator=(SegmentPartition&&) noexcept;

    // --- Lifecycle ---

    Status create(const std::string& path, bool buffered = true);

    // Open and parse footer. Returns segment_id from footer.
    Status open(const std::string& path, uint32_t& out_segment_id);

    // Write SegmentIndex + lineage + footer + fdatasync.
    Status seal(uint32_t segment_id);

    Status close() { return log_file_.close(); }

    // --- Write ---

    Status put(std::string_view key, uint64_t version,
               std::string_view value, bool tombstone, uint64_t hash);

    Status appendEntry(std::string_view key, uint64_t version,
                       std::string_view value, bool tombstone,
                       uint64_t hash, uint64_t& entry_offset);

    Status appendRawEntry(const void* payload, size_t payload_len,
                          uint64_t hash, uint64_t& entry_offset);

    // --- Read ---

    Status getLatest(uint64_t hash, LogEntry& entry) const;
    Status get(uint64_t hash, std::vector<LogEntry>& entries) const;
    Status get(uint64_t hash, uint64_t upper_bound, LogEntry& entry) const;
    bool contains(uint64_t hash) const { return index_.contains(hash); }
    Status readEntry(uint64_t offset, LogEntry& entry) const;

    // Read only the value at offset, skipping key copy. For the common
    // read path where the caller already knows the key.
    Status readValue(uint64_t offset, std::string& value) const;

    // --- Lineage ---

    void setLineageType(LineageType type) { lineage_type_ = type; }
    void reserveLineage(size_t count) { lineage_buf_.reserve(count * 20); }
    void addLineagePresent(uint64_t hkey, uint64_t packed_version,
                           uint32_t old_segment_id = 0);
    void addLineageDeleted(uint64_t hkey, uint64_t packed_version,
                           uint32_t old_segment_id);
    Status readLineage(Lineage& lineage) const;

    // --- Accessors ---

    const LogFile& logFile() const { return log_file_; }
    const SegmentIndex& index() const { return index_; }
    uint64_t dataSize() const { return data_size_; }
    size_t keyCount() const { return index_.keyCount(); }
    size_t entryCount() const { return index_.entryCount(); }
    uint64_t siEncodeCount() const { return index_.encodeCount(); }
    uint64_t siEncodeTotalNs() const { return index_.encodeTotalNs(); }
    uint64_t siDecodeCount() const { return index_.decodeCount(); }
    uint64_t siDecodeTotalNs() const { return index_.decodeTotalNs(); }

private:
    // Read entry at offset, validate CRC, return buffer + parsed lengths.
    Status readRaw(uint64_t offset,
                   uint8_t* stack_buf, size_t stack_size,
                   std::unique_ptr<uint8_t[]>& heap_buf,
                   const uint8_t*& out_buf,
                   uint16_t& out_key_len, uint32_t& out_val_len) const;

    Status writeEntry(std::string_view key, uint64_t version,
                      std::string_view value, bool tombstone,
                      uint64_t& entry_offset);
    void recordBatchEntry(uint64_t hash, uint64_t packed_ver, uint32_t offset);
    void addLineageEntry(uint64_t hkey, uint64_t packed_version);
    Status writeLineageSection();

    static constexpr uint32_t kFooterMagic = 0x53454746;  // "SEGF"
    static constexpr uint32_t kLineageMagic = 0x4C494E47; // "LING"
    // segment_id(4) + index_offset(8) + lineage_offset(8) + magic(4)
    static constexpr size_t kFooterSize = 24;

    LogFile log_file_;
    SegmentIndex index_;
    uint64_t data_size_ = 0;
    uint64_t lineage_offset_ = 0;
    bool batch_mode_ = false;
    LineageType lineage_type_ = LineageType::kFlush;
    std::vector<uint8_t> lineage_buf_;
    uint32_t lineage_present_count_ = 0;
    uint32_t lineage_deleted_count_ = 0;
};

// ---------------------------------------------------------------------------
// Segment: K partitions routed by hash.  K=1 is the single-file case.
// ---------------------------------------------------------------------------

class Segment {
public:
    Segment();
    ~Segment();

    Segment(const Segment&) = delete;
    Segment& operator=(const Segment&) = delete;
    Segment(Segment&&) noexcept;
    Segment& operator=(Segment&&) noexcept;

    enum class State { kClosed, kWriting, kReadable };

    // --- Lifecycle (out-of-line) ---

    Status create(const std::string& path, uint32_t id,
                  uint16_t num_partitions = 1, bool buffered = true);
    Status open(const std::string& path);
    Status seal(class FlushPool* pool = nullptr);

    // Transition to Readable after partitions have been sealed externally
    // (e.g., by parallel flush tasks that called SegmentPartition::seal directly).
    void markSealed() { state_ = State::kReadable; }

    Status close();

    // --- State (inline) ---

    State state() const { return state_; }
    bool isOpen() const { return state_ != State::kClosed; }
    uint32_t getId() const { return id_; }
    uint16_t numPartitions() const { return num_partitions_; }

    // --- Lineage (inline routing) ---

    void setLineageType(LineageType type) {
        lineage_type_ = type;
        for (auto& p : partitions_) p.setLineageType(type);
    }

    // Pre-allocate lineage buffers. total_entries is split evenly across partitions.
    void reserveLineage(size_t total_entries) {
        size_t per_part = (total_entries + num_partitions_ - 1) / num_partitions_;
        for (auto& p : partitions_) p.reserveLineage(per_part);
    }

    void addLineagePresent(uint64_t hkey, uint64_t packed_version,
                           uint32_t old_segment_id = 0) {
        partitions_[partitionFor(hkey)].addLineagePresent(
            hkey, packed_version, old_segment_id);
    }

    void addLineageDeleted(uint64_t hkey, uint64_t packed_version,
                           uint32_t old_segment_id) {
        partitions_[partitionFor(hkey)].addLineageDeleted(
            hkey, packed_version, old_segment_id);
    }

    // Merge lineage from all partitions (out-of-line).
    Status readLineage(Lineage& lineage) const;

    // --- Write (inline dispatch) ---

    Status put(std::string_view key, uint64_t version,
               std::string_view value, bool tombstone, uint64_t hash) {
        if (state_ != State::kWriting)
            return Status::InvalidArgument("Segment: put requires Writing state");
        if (key.size() > LogEntry::kMaxKeyLen)
            return Status::InvalidArgument("Segment: key too long");
        return partitions_[partitionFor(hash)].put(key, version, value, tombstone, hash);
    }

    Status appendEntry(std::string_view key, uint64_t version,
                       std::string_view value, bool tombstone,
                       uint64_t hash, uint64_t& entry_offset) {
        if (state_ != State::kWriting)
            return Status::InvalidArgument("Segment: appendEntry requires Writing state");
        if (key.size() > LogEntry::kMaxKeyLen)
            return Status::InvalidArgument("Segment: appendEntry key too long");
        return partitions_[partitionFor(hash)].appendEntry(
            key, version, value, tombstone, hash, entry_offset);
    }

    Status appendRawEntry(const void* payload, size_t payload_len,
                          uint64_t hash, uint64_t& entry_offset) {
        if (state_ != State::kWriting)
            return Status::InvalidArgument("Segment: appendRawEntry requires Writing state");
        return partitions_[partitionFor(hash)].appendRawEntry(
            payload, payload_len, hash, entry_offset);
    }

    // --- Read (inline dispatch) ---

    Status getLatest(uint64_t hash, LogEntry& entry) const {
        if (state_ != State::kReadable)
            return Status::InvalidArgument("Segment: getLatest requires Readable state");
        return partitions_[partitionFor(hash)].getLatest(hash, entry);
    }

    Status get(uint64_t hash, std::vector<LogEntry>& entries) const {
        if (state_ != State::kReadable)
            return Status::InvalidArgument("Segment: get requires Readable state");
        return partitions_[partitionFor(hash)].get(hash, entries);
    }

    Status get(uint64_t hash, uint64_t upper_bound, LogEntry& entry) const {
        if (state_ != State::kReadable)
            return Status::InvalidArgument("Segment: get requires Readable state");
        return partitions_[partitionFor(hash)].get(hash, upper_bound, entry);
    }

    // Read only the value for a key at the given version bound.
    // Avoids copying the key — more efficient for the common read path.
    Status getValue(uint64_t hash, uint64_t upper_bound, std::string& value) const {
        if (state_ != State::kReadable)
            return Status::InvalidArgument("Segment: getValue requires Readable state");
        auto& p = partitions_[partitionFor(hash)];
        uint64_t offset, pv;
        if (!p.index().get(hash, upper_bound, offset, pv)) {
            return Status::NotFound("key not found");
        }
        return p.readValue(offset, value);
    }

    bool contains(uint64_t hash) const {
        if (state_ != State::kReadable) return false;
        return partitions_[partitionFor(hash)].contains(hash);
    }

    // --- Partition access (inline) ---

    SegmentPartition& partition(uint16_t p) { return partitions_[p]; }
    const SegmentPartition& partition(uint16_t p) const { return partitions_[p]; }

    const LogFile& logFile() const { return partitions_[0].logFile(); }
    const LogFile& logFile(uint16_t p) const { return partitions_[p].logFile(); }
    const SegmentIndex& index() const { return partitions_[0].index(); }
    const SegmentIndex& index(uint16_t p) const { return partitions_[p].index(); }
    uint64_t dataSize(uint16_t p) const { return partitions_[p].dataSize(); }

    Status readEntry(uint16_t p, uint64_t offset, LogEntry& entry) const {
        return partitions_[p].readEntry(offset, entry);
    }

    // --- Aggregate stats (out-of-line) ---

    uint64_t dataSize() const;
    size_t keyCount() const;
    size_t entryCount() const;
    uint64_t siEncodeCount() const;
    uint64_t siEncodeTotalNs() const;
    uint64_t siDecodeCount() const;
    uint64_t siDecodeTotalNs() const;

    // --- Path helper ---

    static std::string partitionPath(const std::string& base_path, uint16_t partition);

private:
    uint16_t partitionFor(uint64_t hash) const {
        if (num_partitions_ == 1) return 0;
        return static_cast<uint16_t>(hash >> (64 - partition_bits_));
    }

    std::vector<SegmentPartition> partitions_;
    uint32_t id_ = 0;
    uint16_t num_partitions_ = 0;
    uint16_t partition_bits_ = 0;
    State state_ = State::kClosed;
    LineageType lineage_type_ = LineageType::kFlush;
    std::string base_path_;
};

// ---------------------------------------------------------------------------
// FlushPool: thread pool for parallel partition sealing.
// ---------------------------------------------------------------------------

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
