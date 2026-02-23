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
    Status create(const std::string& path, uint32_t id);

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
               std::string_view value, bool tombstone);

    // --- Read (Readable only) ---

    // Get the latest entry for a key.
    Status getLatest(const std::string& key, LogEntry& entry) const;

    // Get all entries for a key.
    Status get(const std::string& key, std::vector<LogEntry>& entries) const;

    // Get the entry for a key with the highest version <= upper_bound.
    Status get(const std::string& key, uint64_t upper_bound,
               LogEntry& entry) const;

    // Check if a key exists.
    bool contains(const std::string& key) const;

    // --- Stats (any state) ---

    const LogFile& logFile() const { return log_file_; }
    const SegmentIndex& index() const { return index_; }

    uint64_t dataSize() const;
    size_t keyCount() const;
    size_t entryCount() const;

    // Read and CRC-validate a LogEntry at the given file offset.
    Status readEntry(uint64_t offset, LogEntry& entry) const;

private:
    static constexpr uint32_t kFooterMagic = 0x53454746;  // "SEGF"
    static constexpr size_t kFooterSize = 16;  // segment_id(4) + index_offset(8) + magic(4)

    LogFile log_file_;
    SegmentIndex index_;
    uint32_t id_ = 0;
    uint64_t data_size_ = 0;
    State state_ = State::kClosed;
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_SEGMENT_H
