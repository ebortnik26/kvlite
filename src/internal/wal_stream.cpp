#include "internal/wal_stream.h"

#include <algorithm>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>

#include "internal/crc32.h"

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
    if (body_len < 5) return false;  // minimum: type(1) + crc32(4)

    body_.resize(body_len);
    if (!reader_.readExact(body_.data(), body_len)) return false;

    uint32_t stored_crc;
    std::memcpy(&stored_crc, body_.data() + body_len - 4, 4);
    if (crc32(body_.data(), body_len - 4) != stored_crc) return false;

    type = body_[0];
    payload = body_.data() + 1;
    payload_len = body_len - 5;  // exclude type(1) and crc32(4)
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
        if (remaining < 14) return 0;
        std::memcpy(&out.packed_version, p, 8); p += 8;
        std::memcpy(&out.segment_id, p, 4);     p += 4;
        uint16_t kl;
        std::memcpy(&kl, p, 2);                 p += 2;
        if (remaining - 14 < kl) return 0;
        out.new_segment_id = 0;
        out.key.assign(reinterpret_cast<const char*>(p), kl);
        return 2 + 14 + kl;  // op + producer_id + fixed fields + key
    }

    if (out.op == WalOp::kRelocate) {
        if (remaining < 18) return 0;
        std::memcpy(&out.packed_version, p, 8);   p += 8;
        std::memcpy(&out.segment_id, p, 4);       p += 4;
        std::memcpy(&out.new_segment_id, p, 4);   p += 4;
        uint16_t kl;
        std::memcpy(&kl, p, 2);                   p += 2;
        if (remaining - 18 < kl) return 0;
        out.key.assign(reinterpret_cast<const char*>(p), kl);
        return 2 + 18 + kl;  // op + producer_id + fixed fields + key
    }

    return 0;  // unknown op
}

bool WALReplayStream::readNextBatch() {
    // Accumulate domain records until a WAL-level commit confirms the
    // transaction was fully written. Incomplete transactions are discarded.
    //
    // Each WAL data record may contain multiple concatenated domain records
    // (a bulk write from one producer). Parse all of them from the payload.
    std::vector<OwnedRecord> pending;

    uint8_t type;
    const uint8_t* payload;
    size_t payload_len;

    while (readRawRecord(type, payload, payload_len)) {
        if (type == kTypeData) {
            // Parse all concatenated domain records from this payload.
            const uint8_t* p = payload;
            size_t remaining = payload_len;
            while (remaining > 0) {
                OwnedRecord rec;
                size_t consumed = parseDomainRecord(p, remaining, rec);
                if (consumed == 0) break;  // malformed — skip rest
                pending.push_back(std::move(rec));
                p += consumed;
                remaining -= consumed;
            }
        } else if (type == kTypeCommit) {
            if (!pending.empty()) {
                batch_ = std::move(pending);
                return true;
            }
            // Empty transaction — keep reading.
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
    current_.key = r.key;
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
