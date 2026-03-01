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

void GlobalIndexWAL::loadManifestState() {
    std::string val;
    has_files_ = false;
    file_ids_.clear();

    if (manifest_->get(ManifestKey::kGiWalMinFileId, val)) {
        min_file_id_ = static_cast<uint32_t>(std::stoul(val));
        has_files_ = true;
    }
    if (manifest_->get(ManifestKey::kGiWalMaxFileId, val)) {
        max_file_id_ = static_cast<uint32_t>(std::stoul(val));
        has_files_ = true;
    }

    if (has_files_) {
        for (uint32_t id = min_file_id_; id <= max_file_id_; ++id) {
            file_ids_.push_back(id);
        }
    }
}

Status GlobalIndexWAL::openActiveFile() {
    std::string dir = walDir();
    total_size_ = 0;

    if (!file_ids_.empty()) {
        for (size_t i = 0; i + 1 < file_ids_.size(); ++i) {
            struct stat st;
            if (::stat(walFilePath(dir, file_ids_[i]).c_str(), &st) == 0) {
                total_size_ += static_cast<uint64_t>(st.st_size);
            }
        }
        current_file_id_ = file_ids_.back();
        return wal_.open(walFilePath(dir, current_file_id_));
    }

    uint32_t id;
    Status s = allocateFileId(id);
    if (!s.ok()) return s;
    current_file_id_ = id;
    file_ids_.push_back(id);

    return wal_.create(walFilePath(dir, id));
}

Status GlobalIndexWAL::open(const std::string& db_path, Manifest& manifest,
                             const Options& options) {
    db_path_ = db_path;
    manifest_ = &manifest;
    options_ = options;

    ::mkdir((db_path + "/gi").c_str(), 0755);
    ::mkdir(walDir().c_str(), 0755);

    loadManifestState();

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
    total_size_ += wal_.size();
    uint64_t closing_max = wal_.maxVersion();

    Status s = wal_.close();
    if (!s.ok()) return s;

    // Allocate a new file ID and update max in Manifest.
    uint32_t new_id;
    s = allocateFileId(new_id);
    if (!s.ok()) return s;

    // Create the new file and inherit the running max_version.
    current_file_id_ = new_id;
    file_ids_.push_back(new_id);
    s = wal_.create(walFilePath(walDir(), new_id));
    if (!s.ok()) return s;
    wal_.updateMaxVersion(closing_max);
    return Status::OK();
}

Status GlobalIndexWAL::allocateFileId(uint32_t& file_id) {
    if (!has_files_) {
        file_id = 0;
        min_file_id_ = 0;
        max_file_id_ = 0;
        has_files_ = true;
        Status s = manifest_->set(ManifestKey::kGiWalMinFileId, "0");
        if (!s.ok()) return s;
        return manifest_->set(ManifestKey::kGiWalMaxFileId, "0");
    }
    file_id = max_file_id_ + 1;
    max_file_id_ = file_id;
    return manifest_->set(ManifestKey::kGiWalMaxFileId, std::to_string(file_id));
}

std::unique_ptr<WALStream> GlobalIndexWAL::replayStream() const {
    return stream::walReplay(walDir(), file_ids_);
}

Status GlobalIndexWAL::startNewFile() {
    // Accumulate size of the file we're closing.
    total_size_ += wal_.size();
    uint64_t closing_max = wal_.maxVersion();

    Status s = wal_.close();
    if (!s.ok()) return s;

    // Allocate a new file ID.
    uint32_t new_id;
    s = allocateFileId(new_id);
    if (!s.ok()) return s;

    current_file_id_ = new_id;
    file_ids_.push_back(new_id);
    s = wal_.create(walFilePath(walDir(), new_id));
    if (!s.ok()) return s;
    wal_.updateMaxVersion(closing_max);
    return Status::OK();
}

void GlobalIndexWAL::updateMaxVersion(uint64_t v) {
    wal_.updateMaxVersion(v);
}

Status GlobalIndexWAL::truncate() {
    total_size_ = 0;
    for (auto& p : producers_) {
        p.data.clear();
        p.record_count = 0;
    }
    wal_.abort();
    return Status::OK();
}

Status GlobalIndexWAL::truncate(uint64_t cutoff_version) {
    (void)cutoff_version;

    // Clear staging buffers.
    for (auto& p : producers_) {
        p.data.clear();
        p.record_count = 0;
    }
    wal_.abort();

    // Delete all closed WAL files (all except the active file).
    std::string dir = walDir();
    for (uint32_t fid : file_ids_) {
        if (fid == current_file_id_) continue;
        std::string path = walFilePath(dir, fid);
        std::remove(path.c_str());
    }

    // Update range: only the active file remains.
    file_ids_.clear();
    file_ids_.push_back(current_file_id_);
    min_file_id_ = current_file_id_;
    manifest_->set(ManifestKey::kGiWalMinFileId, std::to_string(min_file_id_));

    total_size_ = 0;
    return Status::OK();
}

uint64_t GlobalIndexWAL::size() const {
    return total_size_ + wal_.size();
}

std::string GlobalIndexWAL::walDir() const {
    return db_path_ + "/gi/wal";
}

std::string GlobalIndexWAL::walFilePath(const std::string& wal_dir, uint32_t file_id) {
    char buf[16];
    std::snprintf(buf, sizeof(buf), "%08u", file_id);
    return wal_dir + "/" + buf + ".log";
}

}  // namespace internal
}  // namespace kvlite
