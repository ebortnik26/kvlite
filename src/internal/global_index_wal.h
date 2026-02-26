#ifndef KVLITE_INTERNAL_L1_WAL_H
#define KVLITE_INTERNAL_L1_WAL_H

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "kvlite/status.h"
#include "internal/wal.h"

namespace kvlite {
namespace internal {

class GlobalIndex;
class Manifest;
class WALStream;

// WAL operation types for GlobalIndex
enum class WalOp : uint8_t {
    kPut       = 1,   // key, packed_version, segment_id
    kRelocate  = 2,   // key, packed_version, old_segment_id, new_segment_id
    kEliminate = 3,   // key, packed_version, segment_id
    kCommit    = 4,   // batch boundary marker (version payload)
};

// Producer IDs for the single multiplexed WAL.
namespace WalProducer {
    constexpr uint8_t kWB = 0;   // WriteBuffer flush
    constexpr uint8_t kGC = 1;   // Garbage collection
}

// Write-Ahead Log for GlobalIndex.
//
// Single multiplexed WAL shared by all producers (WriteBuffer flush, GC).
// Each record carries a producer_id byte for demultiplexing during replay.
//
// Multi-file design: WAL files live in <db_path>/gi/wal/ with names
// NNNNNNNN.log (zero-padded 8-digit file ID). When the active file
// exceeds max_file_size after a commit(), the WAL rolls over to a new
// file. Each file is tracked in the Manifest.
//
// Built on the generic WAL primitive which provides CRC-protected records
// with transaction semantics. Domain payloads (no per-record CRC) are:
//
// kPut / kEliminate (16 + key_len bytes):
//   [op:1][producer_id:1][packed_version:8][segment_id:4][key_len:2][key:var]
//
// kRelocate (20 + key_len bytes):
//   [op:1][producer_id:1][packed_version:8][old_segment_id:4][new_segment_id:4][key_len:2][key:var]
//
// kCommit (10 bytes):
//   [op:1][producer_id:1][version:8]
//
// Thread-safety: Not thread-safe. All batch recording and commit calls must
// be externally serialized (which they are — called only from flush and GC
// merge, both single-threaded producers).
class GlobalIndexWAL {
public:
    struct Options {
        // Maximum size of a single WAL file before rollover (default 1 GB).
        uint64_t max_file_size = 1ULL << 30;
    };

    GlobalIndexWAL();
    ~GlobalIndexWAL();

    // Open WAL directory, recovering state from the Manifest.
    Status open(const std::string& db_path, Manifest& manifest,
                const Options& options);

    // Close the active WAL file.
    Status close();

    // --- Batch recording (in-memory only, no I/O) ---
    // No-ops when the WAL file is not open.

    void appendPut(std::string_view key, uint64_t packed_version,
                   uint32_t segment_id, uint8_t producer_id);

    void appendRelocate(std::string_view key, uint64_t packed_version,
                        uint32_t old_segment_id, uint32_t new_segment_id,
                        uint8_t producer_id);

    void appendEliminate(std::string_view key, uint64_t packed_version,
                         uint32_t segment_id, uint8_t producer_id);

    // Flush batch + commit record to disk in one write.
    // version is the max packed_version covered by this batch.
    // Rolls over to a new file if the current file exceeds max_file_size.
    // No-op when the WAL file is not open.
    Status commit(uint64_t version, uint8_t producer_id, bool sync = false);

    // Return a pull-based stream over all WAL records, ordered by file then position.
    // Each record carries its transaction's commit_version for merge ordering.
    std::unique_ptr<WALStream> replayStream() const;

    // Truncate WAL (resets counters).
    Status truncate();

    // Total WAL size on disk across all files.
    uint64_t size() const;

    // Get number of data entries written (excludes commit records)
    uint64_t entryCount() const { return entry_count_; }

    // Number of WAL files tracked by the Manifest.
    uint32_t fileCount() const { return static_cast<uint32_t>(file_ids_.size()); }

    // Check if WAL is open
    bool isOpen() const { return wal_.isOpen(); }

    // WAL directory path: <db_path>/gi/wal/
    std::string walDir() const;

    // Path for a specific WAL file: <db_path>/wal/NNNNNNNN.log
    static std::string walFilePath(const std::string& wal_dir, uint32_t file_id);

private:
    // Roll over to a new WAL file (close current, allocate next, create new).
    Status rollover();

    // Allocate the next file ID (increments next_file_id_ and persists to Manifest).
    Status allocateFileId(uint32_t& file_id);

    Manifest* manifest_ = nullptr;
    std::string db_path_;
    std::string next_file_id_key_;   // "gi.wal.next_file_id"
    std::string file_prefix_;        // "gi.wal.file."
    Options options_;

    WAL wal_;
    uint64_t entry_count_ = 0;

    // File ID of the currently open WAL file.
    uint32_t current_file_id_ = 0;
    // Next file ID to allocate (persisted in Manifest as "wal.next_file_id").
    uint32_t next_file_id_ = 0;
    // Sum of sizes of all closed WAL files.
    uint64_t total_size_ = 0;
    // Set of all live WAL file IDs (from Manifest "wal.file.<id>" keys).
    std::vector<uint32_t> file_ids_;
};

} // namespace internal
} // namespace kvlite

#endif // KVLITE_INTERNAL_L1_WAL_H
