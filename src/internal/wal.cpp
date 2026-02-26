#include "internal/wal.h"

#include <cstring>

#include "internal/crc32.h"

namespace kvlite {
namespace internal {

WAL::WAL() = default;

WAL::~WAL() {
    if (log_file_.isOpen()) {
        close();
    }
}

WAL::WAL(WAL&& other) noexcept
    : log_file_(std::move(other.log_file_)),
      batch_buf_(std::move(other.batch_buf_)) {}

WAL& WAL::operator=(WAL&& other) noexcept {
    if (this != &other) {
        if (log_file_.isOpen()) {
            close();
        }
        log_file_ = std::move(other.log_file_);
        batch_buf_ = std::move(other.batch_buf_);
    }
    return *this;
}

Status WAL::create(const std::string& path) {
    Status s = log_file_.create(path);
    if (!s.ok()) return s;
    batch_buf_.clear();
    return Status::OK();
}

Status WAL::open(const std::string& path) {
    Status s = log_file_.open(path);
    if (!s.ok()) return s;
    batch_buf_.clear();
    return Status::OK();
}

Status WAL::close() {
    batch_buf_.clear();
    return log_file_.close();
}

bool WAL::isOpen() const {
    return log_file_.isOpen();
}

uint64_t WAL::size() const {
    return log_file_.size();
}

void WAL::put(const void* data, size_t len) {
    // Data record: [body_len:4][type:1][payload:var][crc32:4]
    // body_len = 1 (type) + len (payload) + 4 (crc)
    uint32_t body_len = static_cast<uint32_t>(1 + len + 4);

    size_t offset = batch_buf_.size();
    size_t total = 4 + body_len; // body_len field + body
    batch_buf_.resize(offset + total);
    uint8_t* p = batch_buf_.data() + offset;

    // body_len
    std::memcpy(p, &body_len, 4);
    p += 4;

    // type
    *p = kTypeData;

    // payload
    std::memcpy(p + 1, data, len);

    // crc32 covers type + payload
    uint32_t checksum = crc32(p, 1 + len);
    std::memcpy(p + 1 + len, &checksum, 4);
}

Status WAL::commit(bool do_sync) {
    // Commit record: [body_len:4][type:1][crc32:4]
    uint32_t body_len = 1 + 4; // type + crc = 5
    size_t offset = batch_buf_.size();
    size_t total = 4 + body_len;
    batch_buf_.resize(offset + total);
    uint8_t* p = batch_buf_.data() + offset;

    // body_len
    std::memcpy(p, &body_len, 4);
    p += 4;

    // type
    *p = kTypeCommit;

    // crc32 covers type byte
    uint32_t checksum = crc32(p, 1);
    std::memcpy(p + 1, &checksum, 4);

    // Write entire batch to disk
    uint64_t write_offset;
    Status s = log_file_.append(batch_buf_.data(), batch_buf_.size(), write_offset);
    batch_buf_.clear();

    if (!s.ok()) return s;

    if (do_sync) {
        s = log_file_.sync();
        if (!s.ok()) return s;
    }

    return Status::OK();
}

void WAL::abort() {
    batch_buf_.clear();
}

Status WAL::replay(const ReplayCallback& cb, uint64_t& valid_end) const {
    valid_end = 0;
    uint64_t file_size = log_file_.size();
    if (file_size == 0) return Status::OK();

    // Read the entire file into memory for replay
    std::vector<uint8_t> buf(file_size);
    Status s = log_file_.readAt(0, buf.data(), file_size);
    if (!s.ok()) return s;

    uint64_t pos = 0;
    std::vector<std::string_view> pending;

    while (pos + 5 <= file_size) { // minimum: body_len(4) + type(1)
        // Read body_len
        uint32_t body_len;
        std::memcpy(&body_len, buf.data() + pos, 4);

        // Sanity check: body_len must be at least 5 (type + crc) and
        // the full record must fit in the file
        if (body_len < 5 || pos + 4 + body_len > file_size) {
            break; // truncated record
        }

        uint8_t* body = buf.data() + pos + 4;
        uint8_t type = body[0];

        // CRC covers everything in the body except the trailing 4 bytes (the CRC itself)
        uint32_t expected_crc;
        std::memcpy(&expected_crc, body + body_len - 4, 4);
        uint32_t actual_crc = crc32(body, body_len - 4);

        if (expected_crc != actual_crc) {
            break; // corruption
        }

        if (type == kTypeData) {
            // payload is between type and crc: body[1 .. body_len-4)
            size_t payload_len = body_len - 1 - 4; // subtract type and crc
            const char* payload_ptr = reinterpret_cast<const char*>(body + 1);
            pending.emplace_back(payload_ptr, payload_len);
        } else if (type == kTypeCommit) {
            if (body_len != 5) {
                break; // malformed commit record
            }

            s = cb(pending);
            if (!s.ok()) return s;

            pending.clear();
            valid_end = pos + 4 + body_len;
        } else {
            break; // unknown record type
        }

        pos += 4 + body_len;
    }

    return Status::OK();
}

Status WAL::replayAndTruncate(const ReplayCallback& cb) {
    uint64_t valid_end;
    Status s = replay(cb, valid_end);
    if (!s.ok()) return s;

    if (valid_end < log_file_.size()) {
        s = log_file_.truncateTo(valid_end);
        if (!s.ok()) return s;
    }

    return Status::OK();
}

Status WAL::truncate() {
    batch_buf_.clear();
    return log_file_.truncateTo(0);
}

Status WAL::sync() {
    return log_file_.sync();
}

} // namespace internal
} // namespace kvlite
