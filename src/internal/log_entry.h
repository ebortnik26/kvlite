#ifndef KVLITE_INTERNAL_LOG_ENTRY_H
#define KVLITE_INTERNAL_LOG_ENTRY_H

#include <cstdint>
#include <string>

namespace kvlite {
namespace internal {

// Packed version: stores version (63 bits) + tombstone flag (LSB) in 64 bits.
// Used by LogEntry, WriteBuffer::Entry, and anywhere version+tombstone are stored together.
//
// Layout: [63-bit logical version | 1-bit tombstone]
//   packed = (logical_version << 1) | tombstone
//
// LSB encoding preserves ordering: packed values for different logical
// versions compare correctly with plain < and <=. Snapshot upper bounds
// use (snapshot_version << 1) | 1 to include tombstones at that version.
struct PackedVersion {
    uint64_t data = 0;

    static constexpr uint64_t kTombstoneMask = 1;
    static constexpr uint64_t kVersionShift = 1;

    PackedVersion() = default;
    PackedVersion(uint64_t version, bool tombstone)
        : data((version << kVersionShift) | (tombstone ? 1 : 0)) {}
    explicit PackedVersion(uint64_t packed) : data(packed) {}

    uint64_t version() const { return data >> kVersionShift; }
    bool tombstone() const { return (data & kTombstoneMask) != 0; }

    bool operator<(const PackedVersion& other) const {
        return data < other.data;
    }
};

// Log entry stored in data files.
//
// On-disk format:
// ┌──────────────┬─────────┬───────────┬─────┬───────┬──────────┐
// │ packed_ver   │ key_len │ value_len │ key │ value │ checksum │
// │   8 bytes    │ 2 bytes │  4 bytes  │ var │  var  │ 4 bytes  │
// └──────────────┴─────────┴───────────┴─────┴───────┴──────────┘
//
// packed_ver: (logical_version << 1) | tombstone_bit
// key_len: full 16 bits for length (max 65535).
// Total header size: 14 bytes (before key/value)
// Checksum: CRC32 of all preceding bytes
struct LogEntry {
    PackedVersion pv;
    std::string key;
    std::string value;

    uint64_t version() const { return pv.version(); }
    bool tombstone() const { return pv.tombstone(); }

    size_t serializedSize() const {
        return kHeaderSize + key.size() + value.size() + kChecksumSize;
    }

    static constexpr size_t kHeaderSize = 8 + 2 + 4;  // packed_ver + key_len + value_len
    static constexpr size_t kChecksumSize = 4;
    static constexpr size_t kMaxKeyLen = 0xFFFF;      // 16 bits = 65535
};

// Index entry stored in the SegmentIndex.
// location = file_id (which log file), 32 bits is sufficient.
struct IndexEntry {
    uint64_t version = 0;
    uint32_t location = 0;

    bool operator<(const IndexEntry& other) const {
        return version < other.version;
    }
};

} // namespace internal
} // namespace kvlite

#endif // KVLITE_INTERNAL_LOG_ENTRY_H
