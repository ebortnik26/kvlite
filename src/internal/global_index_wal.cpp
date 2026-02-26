#include "internal/global_index_wal.h"

#include <cstring>

#include "internal/crc32.h"

namespace kvlite {
namespace internal {

GlobalIndexWAL::GlobalIndexWAL() = default;

GlobalIndexWAL::~GlobalIndexWAL() {
    if (log_file_.isOpen()) {
        close();
    }
}

Status GlobalIndexWAL::open(const std::string& path) {
    return log_file_.create(path);
}

Status GlobalIndexWAL::close() {
    batch_buf_.clear();
    return log_file_.close();
}

// --- Batch recording (in-memory, no I/O) ---

void GlobalIndexWAL::appendPut(std::string_view key, uint64_t packed_version,
                                uint32_t segment_id) {
    if (!log_file_.isOpen()) return;
    serializeRecord(WalOp::kPut, key, packed_version, segment_id, 0);
    ++entry_count_;
}

void GlobalIndexWAL::appendRelocate(std::string_view key, uint64_t packed_version,
                                     uint32_t old_segment_id, uint32_t new_segment_id) {
    if (!log_file_.isOpen()) return;
    serializeRecord(WalOp::kRelocate, key, packed_version, old_segment_id, new_segment_id);
    ++entry_count_;
}

void GlobalIndexWAL::appendEliminate(std::string_view key, uint64_t packed_version,
                                      uint32_t segment_id) {
    if (!log_file_.isOpen()) return;
    serializeRecord(WalOp::kEliminate, key, packed_version, segment_id, 0);
    ++entry_count_;
}

void GlobalIndexWAL::serializeRecord(WalOp op, std::string_view key,
                                      uint64_t packed_version,
                                      uint32_t segment_id,
                                      uint32_t new_segment_id) {
    // Compute record body size (excluding CRC)
    size_t body_size = 1; // op
    if (op == WalOp::kCommit) {
        body_size += 8; // version
    } else {
        body_size += 8; // packed_version
        body_size += 4; // segment_id
        if (op == WalOp::kRelocate) {
            body_size += 4; // new_segment_id
        }
        body_size += 2; // key_len
        body_size += key.size();
    }
    size_t total = body_size + 4; // + crc32

    size_t offset = batch_buf_.size();
    batch_buf_.resize(offset + total);
    uint8_t* p = batch_buf_.data() + offset;

    // op
    *p++ = static_cast<uint8_t>(op);

    if (op == WalOp::kCommit) {
        // version (packed_version param carries the commit version)
        std::memcpy(p, &packed_version, 8); p += 8;
    } else {
        // packed_version
        std::memcpy(p, &packed_version, 8); p += 8;
        // segment_id (or old_segment_id for relocate)
        std::memcpy(p, &segment_id, 4); p += 4;
        if (op == WalOp::kRelocate) {
            std::memcpy(p, &new_segment_id, 4); p += 4;
        }
        // key_len + key
        uint16_t key_len = static_cast<uint16_t>(key.size());
        std::memcpy(p, &key_len, 2); p += 2;
        std::memcpy(p, key.data(), key.size()); p += key.size();
    }

    // CRC covers all bytes before the checksum
    uint32_t checksum = crc32(batch_buf_.data() + offset, body_size);
    std::memcpy(p, &checksum, 4);
}

Status GlobalIndexWAL::commit(uint64_t version, bool sync) {
    if (!log_file_.isOpen()) return Status::OK();

    // Append kCommit marker with version to the batch
    serializeRecord(WalOp::kCommit, {}, version, 0, 0);

    // Write entire batch to disk in one append
    uint64_t offset;
    Status s = log_file_.append(batch_buf_.data(), batch_buf_.size(), offset);
    batch_buf_.clear();
    if (!s.ok()) return s;

    if (sync) {
        s = log_file_.sync();
        if (!s.ok()) return s;
    }

    return Status::OK();
}

Status GlobalIndexWAL::replay(GlobalIndex& /*index*/) {
    // Stub — recovery implementation deferred.
    return Status::OK();
}

Status GlobalIndexWAL::truncate() {
    std::string path = log_file_.path();
    Status s = log_file_.close();
    if (!s.ok()) return s;
    s = log_file_.create(path);
    if (!s.ok()) return s;
    entry_count_ = 0;
    batch_buf_.clear();
    return Status::OK();
}

uint64_t GlobalIndexWAL::size() const {
    return log_file_.size();
}

std::string GlobalIndexWAL::makePath(const std::string& db_path) {
    return db_path + "/global_index.wal";
}

}  // namespace internal
}  // namespace kvlite
