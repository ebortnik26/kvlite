#include "internal/wal_stream.h"

#include <algorithm>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>

#include "internal/crc32.h"

// Commit record body: [type:1][txn_crc32:4]
static constexpr size_t kCommitBodyLen = 5;

namespace kvlite {
namespace internal {

// WAL-level record types (mirrored from WAL's private constants).
static constexpr uint8_t kTypeData = 0x01;
static constexpr uint8_t kTypeCommit = 0x02;

// ---------------------------------------------------------------------------
// FileReader
// ---------------------------------------------------------------------------

WALReplayStream::FileReader::FileReader()
    : buf(new uint8_t[kBufSize]) {}

bool WALReplayStream::FileReader::open(const std::string& path) {
    fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return false;
    buf_len = 0;
    buf_pos = 0;
    return true;
}

void WALReplayStream::FileReader::close() {
    if (fd >= 0) { ::close(fd); fd = -1; }
    buf_len = 0;
    buf_pos = 0;
}

bool WALReplayStream::FileReader::readExact(void* dst, size_t len) {
    uint8_t* out = static_cast<uint8_t*>(dst);
    while (len > 0) {
        if (buf_pos < buf_len) {
            size_t avail = buf_len - buf_pos;
            size_t n = std::min(avail, len);
            std::memcpy(out, buf.get() + buf_pos, n);
            buf_pos += n;
            out += n;
            len -= n;
        } else {
            ssize_t r = ::read(fd, buf.get(), kBufSize);
            if (r <= 0) return false;
            buf_len = static_cast<size_t>(r);
            buf_pos = 0;
        }
    }
    return true;
}

// ---------------------------------------------------------------------------
// WALReplayStream
// ---------------------------------------------------------------------------

WALReplayStream::WALReplayStream(const std::string& wal_dir,
                                 const std::vector<uint32_t>& file_ids)
    : wal_dir_(wal_dir), file_ids_(file_ids) {
    // Find the first file with a valid batch.
    while (file_idx_ < file_ids_.size()) {
        if (openNextFile() && readNextBatch()) {
            batch_idx_ = 0;
            updateCurrent();
            valid_ = true;
            return;
        }
        ++file_idx_;
    }
}

WALReplayStream::~WALReplayStream() {
    reader_.close();
}

bool WALReplayStream::valid() const { return valid_; }

const WALRecord& WALReplayStream::record() const { return current_; }

Status WALReplayStream::next() {
    if (!valid_) return Status::OK();

    ++batch_idx_;
    if (batch_idx_ < batch_.size()) {
        updateCurrent();
        return Status::OK();
    }

    // Current batch exhausted — try to read more from the current file.
    if (readNextBatch()) {
        batch_idx_ = 0;
        updateCurrent();
        return Status::OK();
    }

    // Current file exhausted — try next files.
    reader_.close();
    ++file_idx_;
    while (file_idx_ < file_ids_.size()) {
        if (openNextFile() && readNextBatch()) {
            batch_idx_ = 0;
            updateCurrent();
            return Status::OK();
        }
        ++file_idx_;
    }

    valid_ = false;
    return Status::OK();
}

bool WALReplayStream::openNextFile() {
    reader_.close();
    std::string path = GlobalIndexWAL::walFilePath(wal_dir_, file_ids_[file_idx_]);
    return reader_.open(path);
}

bool WALReplayStream::readRawRecord(uint8_t& type,
                                     const uint8_t*& payload,
                                     size_t& payload_len) {
    uint32_t body_len;
    if (!reader_.readExact(&body_len, 4)) return false;
    if (body_len < 1) return false;  // minimum: type(1)

    body_.resize(body_len);
    if (!reader_.readExact(body_.data(), body_len)) return false;

    type = body_[0];
    payload = body_.data() + 1;
    payload_len = body_len - 1;  // exclude type(1)
    return true;
}

size_t WALReplayStream::parseDomainRecord(const uint8_t* payload, size_t len,
                                           OwnedRecord& out) {
    if (len < 2) return 0;  // need op + producer_id

    out.op = static_cast<WalOp>(payload[0]);
    out.producer_id = payload[1];
    const uint8_t* p = payload + 2;
    size_t remaining = len - 2;

    if (out.op == WalOp::kPut || out.op == WalOp::kEliminate) {
        // [packed_version:8][segment_id:4][hkey:8] = 20 bytes
        if (remaining < 20) return 0;
        std::memcpy(&out.packed_version, p, 8); p += 8;
        std::memcpy(&out.segment_id, p, 4);     p += 4;
        std::memcpy(&out.hkey, p, 8);
        out.new_segment_id = 0;
        return 22;  // op(1) + producer_id(1) + 20
    }

    if (out.op == WalOp::kRelocate) {
        // [packed_version:8][old_seg:4][new_seg:4][hkey:8] = 24 bytes
        if (remaining < 24) return 0;
        std::memcpy(&out.packed_version, p, 8);   p += 8;
        std::memcpy(&out.segment_id, p, 4);       p += 4;
        std::memcpy(&out.new_segment_id, p, 4);   p += 4;
        std::memcpy(&out.hkey, p, 8);
        return 26;  // op(1) + producer_id(1) + 24
    }

    return 0;  // unknown op
}

bool WALReplayStream::readNextBatch() {
    // Accumulate domain records until a WAL-level commit confirms the
    // transaction was fully written. Incomplete transactions are discarded.
    //
    // Each WAL data record may contain multiple concatenated domain records
    // (a bulk write from one producer). Parse all of them from the payload.
    //
    // CRC is per-transaction: the commit record carries a txn_crc32 that
    // covers all bytes from the first data record's body_len through the
    // commit's type byte. We accumulate a running CRC over raw bytes
    // (body_len + body) as we read each record, and verify at commit.
    std::vector<OwnedRecord> pending;
    uint32_t running_crc = 0xFFFFFFFFu;

    uint8_t type;
    const uint8_t* payload;
    size_t payload_len;

    while (readRawRecord(type, payload, payload_len)) {
        // body_len that was read by readRawRecord (reconstruct).
        uint32_t body_len = static_cast<uint32_t>(1 + payload_len);

        if (type == kTypeData) {
            // Accumulate CRC over [body_len:4][type:1][payload:var].
            running_crc = updateCrc32(running_crc, &body_len, 4);
            running_crc = updateCrc32(running_crc, body_.data(), body_.size());

            const uint8_t* p = payload;
            size_t remaining = payload_len;
            while (remaining > 0) {
                OwnedRecord rec;
                size_t consumed = parseDomainRecord(p, remaining, rec);
                if (consumed == 0) break;
                pending.push_back(std::move(rec));
                p += consumed;
                remaining -= consumed;
            }
        } else if (type == kTypeCommit) {
            if (body_len != kCommitBodyLen) break;

            // Accumulate CRC over commit's [body_len:4][type:1].
            running_crc = updateCrc32(running_crc, &body_len, 4);
            uint8_t commit_type = kTypeCommit;
            running_crc = updateCrc32(running_crc, &commit_type, 1);

            // Verify against stored txn_crc32 (payload is the 4-byte CRC).
            uint32_t stored_crc;
            std::memcpy(&stored_crc, payload, 4);
            if (finalizeCrc32(running_crc) != stored_crc) break;

            if (!pending.empty()) {
                batch_ = std::move(pending);
                return true;
            }
            // Empty transaction — reset and keep reading.
            pending.clear();
            running_crc = 0xFFFFFFFFu;
        }
    }
    return false;  // EOF or corruption — discard incomplete transaction
}

void WALReplayStream::updateCurrent() {
    const auto& r = batch_[batch_idx_];
    current_.op = r.op;
    current_.producer_id = r.producer_id;
    current_.packed_version = r.packed_version;
    current_.segment_id = r.segment_id;
    current_.new_segment_id = r.new_segment_id;
    current_.hkey = r.hkey;
}

// ---------------------------------------------------------------------------
// Factory functions
// ---------------------------------------------------------------------------

namespace stream {

std::unique_ptr<WALStream> walReplay(
    const std::string& wal_dir,
    const std::vector<uint32_t>& file_ids) {
    return std::make_unique<WALReplayStream>(wal_dir, file_ids);
}

} // namespace stream

} // namespace internal
} // namespace kvlite
