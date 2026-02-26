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
    WalOp op;                // kPut, kRelocate, kEliminate
    uint8_t producer_id;     // WalProducer::kWB or kGC
    uint64_t packed_version;
    uint32_t segment_id;     // for put/eliminate: the segment; for relocate: old_seg
    uint32_t new_segment_id; // for relocate only (0 otherwise)
    std::string_view key;    // points into owned storage
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

// Streams WALRecords from a sequence of WAL files using a 1 MB read-ahead
// buffer. Records are yielded one committed batch at a time — only complete
// WAL transactions (those with a WAL-level commit record) are emitted.
//
// Memory usage is bounded: O(1 MB buffer + largest single batch).
class WALReplayStream : public WALStream {
public:
    WALReplayStream(const std::string& wal_dir,
                    const std::vector<uint32_t>& file_ids);
    ~WALReplayStream();

    bool valid() const override;
    const WALRecord& record() const override;
    Status next() override;

private:
    // 1 MB read-ahead buffered file reader.
    struct FileReader {
        static constexpr size_t kBufSize = 1 << 20;
        int fd = -1;
        std::unique_ptr<uint8_t[]> buf;
        size_t buf_len = 0;
        size_t buf_pos = 0;

        FileReader();
        bool open(const std::string& path);
        void close();
        bool readExact(void* dst, size_t len);
    };

    struct OwnedRecord {
        WalOp op;
        uint8_t producer_id;
        uint64_t packed_version;
        uint32_t segment_id;
        uint32_t new_segment_id;
        std::string key;
    };

    // Read one raw WAL record: body_len + body + CRC check.
    // Returns type byte via out param, payload points into body_.
    // Returns false on EOF or CRC error.
    bool readRawRecord(uint8_t& type, const uint8_t*& payload, size_t& payload_len);

    // Parse one domain record from a payload buffer.
    // Returns bytes consumed, or 0 if the payload is malformed.
    static size_t parseDomainRecord(const uint8_t* payload, size_t len, OwnedRecord& out);

    bool openNextFile();
    bool readNextBatch();
    void updateCurrent();

    std::string wal_dir_;
    std::vector<uint32_t> file_ids_;
    size_t file_idx_ = 0;

    FileReader reader_;
    std::vector<uint8_t> body_;  // reusable buffer for raw WAL record bodies

    // Current batch being yielded.
    std::vector<OwnedRecord> batch_;
    size_t batch_idx_ = 0;

    WALRecord current_;
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
