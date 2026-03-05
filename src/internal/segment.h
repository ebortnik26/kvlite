#ifndef KVLITE_INTERNAL_SEGMENT_H
#define KVLITE_INTERNAL_SEGMENT_H

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "internal/crc32.h"
#include "internal/segment_index.h"
#include "internal/log_entry.h"
#include "internal/log_file.h"

namespace kvlite {
namespace internal {

// A Segment stores log entries and their SegmentIndex in a single file.
//
// File layout:
//   [LogEntry 0] ... [LogEntry N-1]  (data region)
//   [SegmentIndex: magic + entries + crc]
//   [segment_id: 4 bytes]
//   [index_offset: 8 bytes]
//   [footer_magic: 4 bytes]
//
// State machine:
//   Closed  -> Writing   (create)
//   Closed  -> Readable  (open)
//   Writing -> Readable  (seal)
//   Writing -> Closed    (close)
//   Readable -> Closed   (close)
//
// Writing state:  put, seal, close, stats
// Readable state: getLatest, get, contains, close, stats
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

    // Create a new segment file for writing. Closed -> Writing.
    // When buffered=true, writes go through a 1MB userspace buffer.
    Status create(const std::string& path, uint32_t id,
                  bool buffered = true);

    // Open an existing segment file. Closed -> Readable.
    Status open(const std::string& path);

    // Append the SegmentIndex and footer. Writing -> Readable.
    Status seal();

    // Close the file. Writing|Readable -> Closed.
    Status close();

    State state() const { return state_; }
    bool isOpen() const { return state_ != State::kClosed; }
    uint32_t getId() const { return id_; }

    // --- Write (Writing only) ---

    // Serialize a LogEntry (header + key + value + CRC), append to file,
    // and update the SegmentIndex.
    Status put(std::string_view key, uint64_t version,
               std::string_view value, bool tombstone,
               uint64_t hash);

    // Write entry and feed the streaming index builder (no per-entry
    // decode/encode). Call seal() after all appendEntry calls to finalize.
    Status appendEntry(std::string_view key, uint64_t version,
                       std::string_view value, bool tombstone,
                       uint64_t hash, uint64_t& entry_offset);

    // Append a pre-formatted LogEntry payload (header + key + value) directly.
    // Computes and appends CRC32. Used by Memtable::flush for zero-copy writes
    // when the Memtable record layout matches the LogEntry on-disk format.
    Status appendRawEntry(const void* payload, size_t payload_len,
                          uint64_t hash, uint64_t& entry_offset);

    // --- Read (Readable only) ---

    // Get the latest entry for a hash.
    Status getLatest(uint64_t hash, LogEntry& entry) const;

    // Get all entries for a hash.
    Status get(uint64_t hash, std::vector<LogEntry>& entries) const;

    // Get the entry for a hash with the highest version <= upper_bound.
    Status get(uint64_t hash, uint64_t upper_bound,
               LogEntry& entry) const;

    // Check if a hash exists (any version).
    bool contains(uint64_t hash) const;

    // --- Stats (any state) ---

    const LogFile& logFile() const { return log_file_; }
    const SegmentIndex& index() const { return index_; }

    uint64_t dataSize() const;
    size_t keyCount() const;
    size_t entryCount() const;

    // Read and CRC-validate a LogEntry at the given file offset.
    Status readEntry(uint64_t offset, LogEntry& entry) const;

private:
    // Serialize and append a LogEntry, return file offset.
    Status writeEntry(std::string_view key, uint64_t version,
                      std::string_view value, bool tombstone,
                      uint64_t& entry_offset);

    static constexpr uint32_t kFooterMagic = 0x53454746;  // "SEGF"
    static constexpr size_t kFooterSize = 16;  // segment_id(4) + index_offset(8) + magic(4)

    LogFile log_file_;
    SegmentIndex index_;
    uint32_t id_ = 0;
    uint64_t data_size_ = 0;
    State state_ = State::kClosed;
    bool batch_mode_ = false;
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_SEGMENT_H
