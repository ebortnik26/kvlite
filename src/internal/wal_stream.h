#ifndef KVLITE_INTERNAL_WAL_STREAM_H
#define KVLITE_INTERNAL_WAL_STREAM_H

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "internal/global_index_wal.h"
#include "kvlite/status.h"

namespace kvlite {
namespace internal {

// A single WAL replay record (index operation).
// Unlike EntryStream::Entry which carries key/value data, WALRecord carries
// index operations (put, relocate, eliminate) with no value data.
struct WALRecord {
    WalOp op;                // kPut, kRelocate, kEliminate (kCommit is consumed internally)
    uint8_t producer_id;     // WalProducer::kWB or kGC
    uint64_t packed_version;
    uint32_t segment_id;     // for put/eliminate: the segment; for relocate: old_seg
    uint32_t new_segment_id; // for relocate only (0 otherwise)
    std::string_view key;    // points into owned storage
    uint64_t commit_version; // from the kCommit record — ordering key for merge
};

// Abstract pull-based iterator over WAL replay records.
// Mirrors EntryStream (valid/entry/next) but yields WALRecord.
class WALStream {
public:
    virtual ~WALStream() = default;
    virtual bool valid() const = 0;
    virtual const WALRecord& record() const = 0;
    virtual Status next() = 0;
};

// Materializes a single GlobalIndexWAL (all its WAL files) into a pull-based
// stream of WALRecords, ordered by file-then-position within each file.
//
// Strategy: replay one entire WAL file at a time into a vector of OwnedRecords,
// then iterate. When the vector is exhausted, open the next file.
class WALReplayStream : public WALStream {
public:
    WALReplayStream(const std::string& wal_dir,
                    const std::vector<uint32_t>& file_ids);

    bool valid() const override;
    const WALRecord& record() const override;
    Status next() override;

private:
    Status loadNextFile();
    void updateCurrent();

    std::string wal_dir_;
    std::vector<uint32_t> file_ids_;
    size_t file_idx_ = 0;

    // Owned records from the current WAL file.
    struct OwnedRecord {
        WalOp op;
        uint8_t producer_id;
        uint64_t packed_version;
        uint32_t segment_id;
        uint32_t new_segment_id;
        std::string key;           // owns the key data
        uint64_t commit_version;
    };
    std::vector<OwnedRecord> records_;
    size_t rec_idx_ = 0;
    WALRecord current_;            // holds string_view into records_[rec_idx_]
    bool valid_ = false;
};

// Factory functions
namespace stream {

std::unique_ptr<WALStream> walReplay(
    const std::string& wal_dir,
    const std::vector<uint32_t>& file_ids);

} // namespace stream

} // namespace internal
} // namespace kvlite

#endif // KVLITE_INTERNAL_WAL_STREAM_H
