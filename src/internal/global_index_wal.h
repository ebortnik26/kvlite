#ifndef KVLITE_INTERNAL_L1_WAL_H
#define KVLITE_INTERNAL_L1_WAL_H

#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
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
};

// Producer IDs for the single multiplexed WAL.
namespace WalProducer {
    constexpr uint8_t kWB = 0;   // WriteBuffer flush
    constexpr uint8_t kGC = 1;   // Garbage collection
    constexpr uint8_t kMaxProducers = 2;
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
// with transaction semantics.
//
// Each WAL data record contains a batch of concatenated domain records:
//   [record_1][record_2]...[record_N]
//
// Domain record formats (fixed-size, hash-based):
//
// kPut / kEliminate (22 bytes):
//   [op:1][producer_id:1][packed_version:8][segment_id:4][hkey:8]
//
// kRelocate (26 bytes):
//   [op:1][producer_id:1][packed_version:8][old_segment_id:4][new_segment_id:4][hkey:8]
//
// Thread-safety: All public methods are thread-safe. A shared_mutex serves
// as a lifecycle lock: append/commit take a shared lock (concurrent producers),
// close() takes an exclusive lock (waits for in-flight operations, then blocks
// new ones). commit() additionally acquires an inner mutex to serialize WAL
// file writes. After close(), all write operations return IOError.
class GlobalIndexWAL {
public:
    static constexpr uint32_t kDefaultBatchSize = 1024;

    struct Options {
        // Maximum size of a single WAL file before rollover (default 1 GB).
        uint64_t max_file_size = 1ULL << 30;
        // Number of records before auto-commit per producer.
        uint32_t batch_size = kDefaultBatchSize;
    };

    GlobalIndexWAL();
    ~GlobalIndexWAL();

    // Open WAL directory, recovering state from the Manifest.
    Status open(const std::string& db_path, Manifest& manifest,
                const Options& options);

    // Close the active WAL file. Takes exclusive lock to wait for in-flight
    // operations, flushes pending staged records, then closes. After close()
    // (open_ becomes false), all append/commit calls return IOError.
    Status close();

    // --- Batch recording ---
    // Records are staged in per-producer buffers (no I/O, no locking).
    // When the staging buffer reaches batch_size records, auto-commits.
    // Returns an error only if auto-commit fails.

    Status appendPut(uint64_t hkey, uint64_t packed_version,
                     uint32_t segment_id, uint8_t producer_id);

    Status appendRelocate(uint64_t hkey, uint64_t packed_version,
                          uint32_t old_segment_id, uint32_t new_segment_id,
                          uint8_t producer_id);

    Status appendEliminate(uint64_t hkey, uint64_t packed_version,
                           uint32_t segment_id, uint8_t producer_id);

    // Flush a producer's staged records to disk and sync.
    // Acquires the WAL mutex, writes the batch as a single WAL record,
    // commits and fsyncs the WAL transaction.
    Status commit(uint8_t producer_id);

    // Return a pull-based stream over all WAL records, ordered by file then position.
    std::unique_ptr<WALStream> replayStream() const;

    // Update the running max version for the active WAL file.
    void updateMaxVersion(uint64_t v);

    // Truncate WAL (resets staging buffers only, no file deletion).
    Status truncate();

    // Version-based truncation: delete obsolete WAL files whose max_version
    // is <= cutoff_version. The active file is never deleted.
    // Also clears staging buffers.
    Status truncate(uint64_t cutoff_version);

    // Close the active WAL file and start a new one.
    // Used after recovery so replayed files are never appended to.
    Status startNewFile();

    // Total WAL size on disk across all files.
    uint64_t size() const;

    // Number of WAL files tracked by the Manifest.
    uint32_t fileCount() const { return static_cast<uint32_t>(file_ids_.size()); }

    // Check if WAL is open
    bool isOpen() const { return open_; }

    // WAL directory path: <db_path>/gi/wal/
    std::string walDir() const;

    // Path for a specific WAL file: <db_path>/gi/wal/NNNNNNNN.log
    static std::string walFilePath(const std::string& wal_dir, uint32_t file_id);

private:
    // Per-producer staging buffer (serialized domain records, no WAL framing).
    struct ProducerBuf {
        std::vector<uint8_t> data;
        uint32_t record_count = 0;
    };

    // Serialize a domain record into a producer's staging buffer.
    static void serializeRecord(ProducerBuf& buf, WalOp op, uint8_t producer_id,
                                uint64_t packed_version, uint64_t hkey,
                                const uint32_t* seg_ids, size_t seg_count);

    // Flush a producer's staging buffer under the WAL mutex.
    // Caller must hold rw_mu_ in shared mode.
    Status flushProducer(uint8_t producer_id);

    // Roll over to a new WAL file (close current, allocate next, create new).
    // Caller must hold mu_.
    Status rollover();

    // Allocate the next file ID (increments next_file_id_ and persists to Manifest).
    Status allocateFileId(uint32_t& file_id);

    // Load file IDs and next_file_id from Manifest.
    void loadManifestState();

    // Open the last existing WAL file, or create the first one.
    Status openActiveFile();

    Manifest* manifest_ = nullptr;
    std::string db_path_;
    std::string next_file_id_key_;   // "gi.wal.next_file_id"
    std::string file_prefix_;        // "gi.wal.file."
    Options options_;

    // Lifecycle lock: shared for append/commit, exclusive for close.
    std::shared_mutex rw_mu_;
    bool open_ = false;  // protected by rw_mu_

    std::mutex mu_;  // serializes WAL file writes
    WAL wal_;

    // Per-producer staging buffers (indexed by producer_id).
    // Protected by rw_mu_ (shared for owning producer, exclusive for close).
    ProducerBuf producers_[WalProducer::kMaxProducers];

    // File ID of the currently open WAL file.
    uint32_t current_file_id_ = 0;
    // Next file ID to allocate (persisted in Manifest as "gi.wal.next_file_id").
    uint32_t next_file_id_ = 0;
    // Sum of sizes of all closed WAL files.
    uint64_t total_size_ = 0;
    // Set of all live WAL file IDs (from Manifest "gi.wal.file.<id>" keys).
    std::vector<uint32_t> file_ids_;
};

} // namespace internal
} // namespace kvlite

#endif // KVLITE_INTERNAL_L1_WAL_H
