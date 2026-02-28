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
    // Data record: [body_len:4][type:1][payload:var]
    uint32_t body_len = static_cast<uint32_t>(1 + len);

    size_t offset = batch_buf_.size();
    size_t total = 4 + body_len;
    batch_buf_.resize(offset + total);
    uint8_t* p = batch_buf_.data() + offset;

    std::memcpy(p, &body_len, 4);
    p += 4;
    *p = kTypeData;
    std::memcpy(p + 1, data, len);
}

Status WAL::commit(bool do_sync) {
    // Commit record: [body_len:4][type:1][txn_crc32:4]
    // txn_crc32 covers all bytes from batch start through the commit type byte.
    uint32_t body_len = 1 + 4; // type + crc = 5

    // Append body_len + type (covered by CRC).
    size_t commit_offset = batch_buf_.size();
    batch_buf_.resize(commit_offset + 4 + 1);
    uint8_t* p = batch_buf_.data() + commit_offset;
    std::memcpy(p, &body_len, 4);
    p[4] = kTypeCommit;

    // CRC covers entire batch_buf_ so far (all data records + commit header).
    uint32_t checksum = crc32(batch_buf_.data(), batch_buf_.size());

    // Append CRC (not covered by itself).
    size_t crc_offset = batch_buf_.size();
    batch_buf_.resize(crc_offset + 4);
    std::memcpy(batch_buf_.data() + crc_offset, &checksum, 4);

    // Write entire batch to disk.
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
    uint64_t txn_start = 0;
    std::vector<std::string_view> pending;

    while (pos + 5 <= file_size) { // minimum: body_len(4) + type(1)
        uint32_t body_len;
        std::memcpy(&body_len, buf.data() + pos, 4);

        if (body_len < 1 || pos + 4 + body_len > file_size) {
            break; // truncated record
        }

        uint8_t type = buf[pos + 4];

        if (type == kTypeData) {
            // payload is body[1..body_len): no per-record CRC
            size_t payload_len = body_len - 1;
            const char* payload_ptr = reinterpret_cast<const char*>(buf.data() + pos + 5);
            pending.emplace_back(payload_ptr, payload_len);
        } else if (type == kTypeCommit) {
            if (body_len != 5) {
                break; // malformed commit record
            }

            // txn_crc32 covers bytes [txn_start .. pos+5) — all data records
            // plus the commit's body_len(4) and type(1).
            uint32_t expected_crc;
            std::memcpy(&expected_crc, buf.data() + pos + 5, 4);
            uint32_t actual_crc = crc32(buf.data() + txn_start, pos + 5 - txn_start);
            if (expected_crc != actual_crc) {
                break; // corruption
            }

            s = cb(pending);
            if (!s.ok()) return s;

            pending.clear();
            valid_end = pos + 4 + body_len;
            txn_start = valid_end;
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
