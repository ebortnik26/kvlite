#include "internal/global_index_wal.h"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <sys/stat.h>

#include "internal/global_index.h"
#include "internal/manifest.h"
#include "internal/wal_stream.h"

namespace kvlite {
namespace internal {

GlobalIndexWAL::GlobalIndexWAL() = default;

GlobalIndexWAL::~GlobalIndexWAL() {
    if (open_) {
        close();
    }
}

Status GlobalIndexWAL::openActiveFile() {
    std::string dir = files_.walDir();

    const auto& fids = files_.fileIds();
    if (!fids.empty()) {
        for (size_t i = 0; i + 1 < fids.size(); ++i) {
            struct stat st;
            if (::stat(WALFileManager::walFilePath(dir, fids[i]).c_str(), &st) == 0) {
                files_.addClosedFileSize(static_cast<uint64_t>(st.st_size));
            }
        }
        files_.setCurrentFileId(fids.back());
        return wal_.open(WALFileManager::walFilePath(dir, files_.currentFileId()));
    }

    // Create file on disk before persisting to Manifest.
    uint32_t id = files_.nextFileId();
    Status s = wal_.create(WALFileManager::walFilePath(dir, id));
    if (!s.ok()) return s;

    s = files_.persistFileId(id);
    if (!s.ok()) return s;

    files_.recordFileCreated(id);
    return Status::OK();
}

Status GlobalIndexWAL::open(const std::string& db_path, Manifest& manifest,
                             const Options& options) {
    options_ = options;
    files_.open(db_path, manifest);

    ::mkdir((db_path + "/gi").c_str(), 0755);
    ::mkdir(files_.walDir().c_str(), 0755);

    Status s = openActiveFile();
    if (!s.ok()) return s;

    open_ = true;
    return Status::OK();
}

Status GlobalIndexWAL::close() {
    // Exclusive lock waits for all in-flight append/commit operations,
    // then prevents new ones.
    std::unique_lock lock(rw_mu_);
    if (!open_) return Status::OK();
    open_ = false;

    // No producers can be active — flush remaining staged records directly.
    for (uint8_t i = 0; i < WalProducer::kMaxProducers; ++i) {
        if (producers_[i].record_count > 0) {
            wal_.put(producers_[i].data.data(), producers_[i].data.size());
            producers_[i].data.clear();
            producers_[i].record_count = 0;
            Status s = wal_.commit(true);
            if (!s.ok()) {
                wal_.abort();
                wal_.close();
                return s;
            }
        }
    }
    wal_.abort();
    return wal_.close();
}

// --- Per-producer staging ---

void GlobalIndexWAL::serializeRecord(ProducerBuf& buf, WalOp op,
                                      uint8_t producer_id,
                                      uint64_t packed_version,
                                      uint64_t hkey,
                                      const uint32_t* seg_ids,
                                      size_t seg_count) {
    size_t rec_len = 1 + 1 + 8 + seg_count * 4 + 8;
    size_t off = buf.data.size();
    buf.data.resize(off + rec_len);
    uint8_t* p = buf.data.data() + off;

    *p++ = static_cast<uint8_t>(op);
    *p++ = producer_id;
    std::memcpy(p, &packed_version, 8); p += 8;
    for (size_t i = 0; i < seg_count; ++i) {
        std::memcpy(p, &seg_ids[i], 4); p += 4;
    }
    std::memcpy(p, &hkey, 8);

    ++buf.record_count;
}

Status GlobalIndexWAL::appendPut(uint64_t hkey, uint64_t packed_version,
                                  uint32_t segment_id, uint8_t producer_id) {
    std::shared_lock lock(rw_mu_);
    if (!open_) return Status::IOError("WAL not open");
    serializeRecord(producers_[producer_id], WalOp::kPut, producer_id,
                    packed_version, hkey, &segment_id, 1);

    if (producers_[producer_id].record_count >= options_.batch_size) {
        return flushProducer(producer_id);
    }
    return Status::OK();
}

Status GlobalIndexWAL::stagePut(uint64_t hkey, uint64_t packed_version,
                                 uint32_t segment_id, uint8_t producer_id) {
    std::shared_lock lock(rw_mu_);
    if (!open_) return Status::IOError("WAL not open");
    serializeRecord(producers_[producer_id], WalOp::kPut, producer_id,
                    packed_version, hkey, &segment_id, 1);
    return Status::OK();
}

Status GlobalIndexWAL::appendRelocate(uint64_t hkey, uint64_t packed_version,
                                       uint32_t old_segment_id, uint32_t new_segment_id,
                                       uint8_t producer_id) {
    std::shared_lock lock(rw_mu_);
    if (!open_) return Status::IOError("WAL not open");
    uint32_t segs[2] = {old_segment_id, new_segment_id};
    serializeRecord(producers_[producer_id], WalOp::kRelocate, producer_id,
                    packed_version, hkey, segs, 2);

    if (producers_[producer_id].record_count >= options_.batch_size) {
        return flushProducer(producer_id);
    }
    return Status::OK();
}

Status GlobalIndexWAL::stageRelocate(uint64_t hkey, uint64_t packed_version,
                                      uint32_t old_segment_id, uint32_t new_segment_id,
                                      uint8_t producer_id) {
    std::shared_lock lock(rw_mu_);
    if (!open_) return Status::IOError("WAL not open");
    uint32_t segs[2] = {old_segment_id, new_segment_id};
    serializeRecord(producers_[producer_id], WalOp::kRelocate, producer_id,
                    packed_version, hkey, segs, 2);
    return Status::OK();
}

Status GlobalIndexWAL::appendEliminate(uint64_t hkey, uint64_t packed_version,
                                        uint32_t segment_id, uint8_t producer_id) {
    std::shared_lock lock(rw_mu_);
    if (!open_) return Status::IOError("WAL not open");
    serializeRecord(producers_[producer_id], WalOp::kEliminate, producer_id,
                    packed_version, hkey, &segment_id, 1);

    if (producers_[producer_id].record_count >= options_.batch_size) {
        return flushProducer(producer_id);
    }
    return Status::OK();
}

Status GlobalIndexWAL::stageEliminate(uint64_t hkey, uint64_t packed_version,
                                       uint32_t segment_id, uint8_t producer_id) {
    std::shared_lock lock(rw_mu_);
    if (!open_) return Status::IOError("WAL not open");
    serializeRecord(producers_[producer_id], WalOp::kEliminate, producer_id,
                    packed_version, hkey, &segment_id, 1);
    return Status::OK();
}

Status GlobalIndexWAL::commit(uint8_t producer_id) {
    std::shared_lock lock(rw_mu_);
    if (!open_) return Status::IOError("WAL not open");
    if (producers_[producer_id].record_count == 0) return Status::OK();
    return flushProducer(producer_id);
}

Status GlobalIndexWAL::flushProducer(uint8_t producer_id) {
    ProducerBuf& buf = producers_[producer_id];

    std::lock_guard<std::mutex> lock(mu_);

    // Write entire staging buffer as a single WAL data record.
    wal_.put(buf.data.data(), buf.data.size());
    buf.data.clear();
    buf.record_count = 0;

    Status s = wal_.commit(true);
    if (!s.ok()) return s;

    // Rollover if the current file exceeds the size limit.
    if (wal_.size() >= options_.max_file_size) {
        s = rollover();
        if (!s.ok()) return s;
    }

    return Status::OK();
}

Status GlobalIndexWAL::rollover() {
    // Accumulate size of the file we're closing.
    files_.addClosedFileSize(wal_.size());
    uint64_t closing_max = wal_.maxVersion();

    Status s = wal_.close();
    if (!s.ok()) return s;

    // Create the file on disk BEFORE updating the Manifest.
    uint32_t new_id = files_.nextFileId();
    s = wal_.create(WALFileManager::walFilePath(files_.walDir(), new_id));
    if (!s.ok()) return s;

    s = files_.persistFileId(new_id);
    if (!s.ok()) return s;

    files_.recordFileCreated(new_id);
    wal_.updateMaxVersion(closing_max);
    return Status::OK();
}

std::unique_ptr<WALStream> GlobalIndexWAL::replayStream() const {
    return stream::walReplay(files_.walDir(), files_.fileIds());
}

Status GlobalIndexWAL::startNewFile() {
    std::lock_guard<std::mutex> lock(mu_);

    // Accumulate size of the file we're closing.
    files_.addClosedFileSize(wal_.size());
    uint64_t closing_max = wal_.maxVersion();

    Status s = wal_.close();
    if (!s.ok()) return s;

    // Create the file on disk BEFORE updating the Manifest.
    uint32_t new_id = files_.nextFileId();
    s = wal_.create(WALFileManager::walFilePath(files_.walDir(), new_id));
    if (!s.ok()) return s;

    s = files_.persistFileId(new_id);
    if (!s.ok()) return s;

    files_.recordFileCreated(new_id);
    wal_.updateMaxVersion(closing_max);
    return Status::OK();
}

void GlobalIndexWAL::updateMaxVersion(uint64_t v) {
    wal_.updateMaxVersion(v);
}

Status GlobalIndexWAL::truncate(uint64_t cutoff_version) {
    (void)cutoff_version;

    // Clear staging buffers.
    for (auto& p : producers_) {
        p.data.clear();
        p.record_count = 0;
    }
    wal_.abort();

    files_.truncateToActive();
    return Status::OK();
}

Status GlobalIndexWAL::truncateForSavepoint(uint64_t cutoff_version) {
    (void)cutoff_version;

    // Only clear GC staging buffer (GC is blocked by exclusive savepoint_mu_).
    // WB staging buffer may be in use by concurrent flush — leave it alone.
    producers_[WalProducer::kGC].data.clear();
    producers_[WalProducer::kGC].record_count = 0;

    std::lock_guard<std::mutex> lock(mu_);  // serialize with flushProducer
    wal_.abort();

    files_.truncateToActive();
    return Status::OK();
}

uint64_t GlobalIndexWAL::size() const {
    return files_.totalSize() + wal_.size();
}

}  // namespace internal
}  // namespace kvlite
