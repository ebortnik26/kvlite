#ifndef KVLITE_INTERNAL_LOG_ENTRY_H
#define KVLITE_INTERNAL_LOG_ENTRY_H

#include <cstdint>
#include <string>

namespace kvlite {
namespace internal {

// Packed version: stores version (63 bits) + tombstone flag (MSB) in 64 bits.
// Used by LogEntry, WriteBuffer::Entry, and anywhere version+tombstone are stored together.
struct PackedVersion {
    uint64_t data = 0;

    static constexpr uint64_t kTombstoneMask = 1ULL << 63;
    static constexpr uint64_t kVersionMask = ~kTombstoneMask;

    PackedVersion() = default;
    PackedVersion(uint64_t version, bool tombstone)
        : data(tombstone ? (version | kTombstoneMask) : version) {}
    explicit PackedVersion(uint64_t packed) : data(packed) {}

    uint64_t version() const { return data & kVersionMask; }
    bool tombstone() const { return (data & kTombstoneMask) != 0; }

    bool operator<(const PackedVersion& other) const {
        return version() < other.version();
    }
};

// Log entry stored in data files.
//
// On-disk format:
// ┌──────────────────┬─────────┬───────────┬─────┬───────┬──────────┐
// │ version|tombstone│ key_len │ value_len │ key │ value │ checksum │
// │      8 bytes     │ 4 bytes │  4 bytes  │ var │  var  │ 4 bytes  │
// └──────────────────┴─────────┴───────────┴─────┴───────┴──────────┘
//
// Total header size: 16 bytes (before key/value)
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

    static constexpr size_t kHeaderSize = 8 + 4 + 4;  // packed_version + key_len + value_len
    static constexpr size_t kChecksumSize = 4;
};

// Index entry stored in L1 and L2 indices.
// L1: location = file_id (which log file)
// L2: location = offset (byte offset within log file)
struct IndexEntry {
    uint64_t version = 0;
    uint64_t location = 0;

    bool operator<(const IndexEntry& other) const {
        return version < other.version;
    }
};

} // namespace internal
} // namespace kvlite

#endif // KVLITE_INTERNAL_LOG_ENTRY_H
