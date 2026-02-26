#ifndef KVLITE_INTERNAL_L1_WAL_H
#define KVLITE_INTERNAL_L1_WAL_H

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "kvlite/status.h"
#include "internal/log_file.h"

namespace kvlite {
namespace internal {

class GlobalIndex;

// WAL operation types for GlobalIndex
enum class WalOp : uint8_t {
    kPut       = 1,   // key, packed_version, segment_id
    kRelocate  = 2,   // key, packed_version, old_segment_id, new_segment_id
    kEliminate = 3,   // key, packed_version, segment_id
    kCommit    = 4,   // batch boundary marker (version payload)
};

// Write-Ahead Log for GlobalIndex.
//
// Batch-buffered design: append methods accumulate serialized records in an
// in-memory buffer (no I/O). commit() appends a kCommit marker, writes the
// entire batch to disk in a single LogFile::append(), and clears the buffer.
//
// On-disk record format (variable-length, per-record CRC):
//
// kPut / kEliminate (19 + key_len bytes):
//   [op:1][packed_version:8][segment_id:4][key_len:2][key:var][crc32:4]
//
// kRelocate (23 + key_len bytes):
//   [op:1][packed_version:8][old_segment_id:4][new_segment_id:4][key_len:2][key:var][crc32:4]
//
// kCommit (13 bytes):
//   [op:1][version:8][crc32:4]
//
// CRC covers all bytes of the record preceding the checksum field.
//
// Thread-safety: Not thread-safe. All batch recording and commit calls must
// be externally serialized (which they are — called only from flush and GC
// merge, both single-threaded producers).
class GlobalIndexWAL {
public:
    GlobalIndexWAL();
    ~GlobalIndexWAL();

    // Open or create WAL file
    Status open(const std::string& path);

    // Close WAL file
    Status close();

    // --- Batch recording (in-memory only, no I/O) ---
    // No-ops when the WAL file is not open.

    void appendPut(std::string_view key, uint64_t packed_version,
                   uint32_t segment_id);

    void appendRelocate(std::string_view key, uint64_t packed_version,
                        uint32_t old_segment_id, uint32_t new_segment_id);

    void appendEliminate(std::string_view key, uint64_t packed_version,
                         uint32_t segment_id);

    // Flush batch + commit record to disk in one write.
    // version is the max packed_version covered by this batch.
    // No-op when the WAL file is not open.
    Status commit(uint64_t version, bool sync = false);

    // Replay WAL entries into a GlobalIndex (stub — deferred)
    Status replay(GlobalIndex& index);

    // Truncate WAL (called after successful snapshot)
    Status truncate();

    // Get current WAL file size on disk
    uint64_t size() const;

    // Get number of data entries written (excludes commit records)
    uint64_t entryCount() const { return entry_count_; }

    // Check if WAL is open
    bool isOpen() const { return log_file_.isOpen(); }

    // WAL file path helper
    static std::string makePath(const std::string& db_path);

private:
    void serializeRecord(WalOp op, std::string_view key,
                         uint64_t packed_version,
                         uint32_t segment_id,
                         uint32_t new_segment_id);

    LogFile log_file_;
    std::vector<uint8_t> batch_buf_;
    uint64_t entry_count_ = 0;
};

} // namespace internal
} // namespace kvlite

#endif // KVLITE_INTERNAL_L1_WAL_H
