#ifndef KVLITE_INTERNAL_LOG_ENTRY_H
#define KVLITE_INTERNAL_LOG_ENTRY_H

#include <cstdint>
#include <string>

namespace kvlite {
namespace internal {

// Log entry stored in data files.
//
// On-disk format:
// ┌──────────────────┬─────────┬───────────┬─────┬───────┬──────────┐
// │ version|tombstone│ key_len │ value_len │ key │ value │ checksum │
// │      8 bytes     │ 4 bytes │  4 bytes  │ var │  var  │ 4 bytes  │
// └──────────────────┴─────────┴───────────┴─────┴───────┴──────────┘
//
// Version encoding: MSB is tombstone flag, lower 63 bits are version.
// Max version: 2^63 - 1 (still effectively unlimited)
//
// Total header size: 16 bytes (before key/value)
// Checksum: CRC32 of all preceding bytes

struct LogEntry {
    uint64_t version = 0;
    std::string key;
    std::string value;
    bool tombstone = false;

    // Pack version and tombstone into single 64-bit value for serialization
    uint64_t packedVersion() const {
        return tombstone ? (version | kTombstoneMask) : version;
    }

    // Unpack version and tombstone from serialized 64-bit value
    static void unpackVersion(uint64_t packed, uint64_t& version, bool& tombstone) {
        tombstone = (packed & kTombstoneMask) != 0;
        version = packed & kVersionMask;
    }

    // Calculate serialized size
    size_t serializedSize() const {
        return kHeaderSize + key.size() + value.size() + kChecksumSize;
    }

    static constexpr uint64_t kTombstoneMask = 1ULL << 63;
    static constexpr uint64_t kVersionMask = ~kTombstoneMask;
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
