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
    if (manifest_->get(next_file_id_key_, val)) {
        next_file_id_ = static_cast<uint32_t>(std::stoul(val));
    } else {
        next_file_id_ = 0;
    }

    file_ids_.clear();
    auto keys = manifest_->getKeysWithPrefix(file_prefix_);
    for (const auto& key : keys) {
        std::string id_str = key.substr(file_prefix_.size());
        file_ids_.push_back(static_cast<uint32_t>(std::stoul(id_str)));
    }
    std::sort(file_ids_.begin(), file_ids_.end());
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

    s = manifest_->set(file_prefix_ + std::to_string(id), "");
    if (!s.ok()) return s;

    return wal_.create(walFilePath(dir, id));
}

Status GlobalIndexWAL::open(const std::string& db_path, Manifest& manifest,
                             const Options& options) {
    db_path_ = db_path;
    manifest_ = &manifest;
    options_ = options;
    next_file_id_key_ = "gi.wal.next_file_id";
    file_prefix_ = "gi.wal.file.";

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
    // Persist the active file's max_version to Manifest.
    manifest_->set(file_prefix_ + std::to_string(current_file_id_),
                   std::to_string(wal_.maxVersion()));

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

    // Persist the closing file's max_version to Manifest.
    uint64_t closing_max = wal_.maxVersion();
    Status s = manifest_->set(file_prefix_ + std::to_string(current_file_id_),
                              std::to_string(closing_max));
    if (!s.ok()) return s;

    s = wal_.close();
    if (!s.ok()) return s;

    // Allocate a new file ID.
    uint32_t new_id;
    s = allocateFileId(new_id);
    if (!s.ok()) return s;

    // Register in Manifest.
    s = manifest_->set(file_prefix_ + std::to_string(new_id), "");
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
    file_id = next_file_id_++;
    return manifest_->set(next_file_id_key_, std::to_string(next_file_id_));
}

std::unique_ptr<WALStream> GlobalIndexWAL::replayStream() const {
    return stream::walReplay(walDir(), file_ids_);
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
    // Clear staging buffers.
    for (auto& p : producers_) {
        p.data.clear();
        p.record_count = 0;
    }
    wal_.abort();

    // Delete obsolete WAL files (all except the active file)
    // whose max_version <= cutoff_version.
    std::string dir = walDir();
    std::vector<uint32_t> kept;
    for (uint32_t fid : file_ids_) {
        if (fid == current_file_id_) {
            kept.push_back(fid);
            continue;
        }

        // Read max_version from Manifest.
        std::string key = file_prefix_ + std::to_string(fid);
        std::string val;
        uint64_t file_max = 0;
        if (manifest_->get(key, val) && !val.empty()) {
            file_max = std::stoull(val);
        }

        if (file_max <= cutoff_version) {
            // Delete file from disk.
            std::string path = walFilePath(dir, fid);
            std::remove(path.c_str());
            // Remove from Manifest.
            manifest_->remove(key);
        } else {
            kept.push_back(fid);
        }
    }
    file_ids_ = std::move(kept);
    total_size_ = 0;

    // Recompute total_size_ from remaining closed files.
    for (uint32_t fid : file_ids_) {
        if (fid == current_file_id_) continue;
        struct stat st;
        if (::stat(walFilePath(dir, fid).c_str(), &st) == 0) {
            total_size_ += static_cast<uint64_t>(st.st_size);
        }
    }

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
