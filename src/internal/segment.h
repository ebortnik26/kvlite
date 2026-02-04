#ifndef KVLITE_INTERNAL_SEGMENT_H
#define KVLITE_INTERNAL_SEGMENT_H

#include <cstdint>
#include <string>
#include <vector>

#include "internal/l2_index.h"
#include "internal/log_file.h"

namespace kvlite {
namespace internal {

// A Segment stores log entries and their L2 index in a single file.
//
// File layout:
//   [LogEntry 0] ... [LogEntry N-1]  (data region)
//   [L2 Index: magic + entries + crc]
//   [index_offset: 8 bytes]
//   [footer_magic: 4 bytes]
//
// State machine:
//   Closed  → Writing   (create)
//   Closed  → Readable  (open)
//   Writing → Readable  (seal)
//   Writing → Closed    (close)
//   Readable → Closed   (close)
//
// Writing state:  append, addIndex, seal, close, stats
// Readable state: readAt, index queries, close, stats
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

    // Create a new segment file for writing. Closed → Writing.
    Status create(const std::string& path);

    // Open an existing segment file. Closed → Readable.
    Status open(const std::string& path);

    // Append the L2 index and footer. Writing → Readable.
    Status seal();

    // Close the file. Writing|Readable → Closed.
    Status close();

    State state() const { return state_; }
    bool isOpen() const { return state_ != State::kClosed; }

    // --- Write (Writing only) ---

    Status append(const void* data, size_t len, uint64_t& offset);
    Status addIndex(const std::string& key, uint32_t offset, uint32_t version);

    // --- Read (Readable only) ---

    Status readAt(uint64_t offset, void* buf, size_t len);

    // --- Index queries (Readable only) ---

    bool getLatest(const std::string& key,
                   uint32_t& offset, uint32_t& version) const;

    bool get(const std::string& key,
             std::vector<uint32_t>& offsets,
             std::vector<uint32_t>& versions) const;

    bool get(const std::string& key, uint64_t upper_bound,
             uint64_t& offset, uint64_t& version) const;

    bool contains(const std::string& key) const;

    // --- Stats (any state) ---

    uint64_t dataSize() const;
    size_t keyCount() const;
    size_t entryCount() const;

private:
    static constexpr uint32_t kFooterMagic = 0x53454746;  // "SEGF"
    static constexpr size_t kFooterSize = 12;  // index_offset(8) + magic(4)

    LogFile log_file_;
    L2Index index_;
    uint64_t data_size_ = 0;
    State state_ = State::kClosed;
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_SEGMENT_H
