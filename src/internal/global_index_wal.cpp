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
    if (wal_.isOpen()) {
        close();
    }
}

Status GlobalIndexWAL::open(const std::string& db_path, Manifest& manifest,
                             const Options& options, const std::string& name) {
    db_path_ = db_path;
    manifest_ = &manifest;
    options_ = options;
    name_ = name;
    next_file_id_key_ = "wal." + name + ".next_file_id";
    file_prefix_ = "wal." + name + ".file.";

    // Create wal/ parent, then wal/<name>/ child.
    std::string parent = db_path + "/wal";
    ::mkdir(parent.c_str(), 0755);
    std::string dir = walDir();
    ::mkdir(dir.c_str(), 0755);

    // Read next_file_id from Manifest (0 if absent).
    std::string val;
    if (manifest_->get(next_file_id_key_, val)) {
        next_file_id_ = static_cast<uint32_t>(std::stoul(val));
    } else {
        next_file_id_ = 0;
    }

    // Enumerate existing WAL files from Manifest.
    file_ids_.clear();
    auto keys = manifest_->getKeysWithPrefix(file_prefix_);
    for (const auto& key : keys) {
        // key = "wal.<name>.file.<id>"
        std::string id_str = key.substr(file_prefix_.size());
        uint32_t id = static_cast<uint32_t>(std::stoul(id_str));
        file_ids_.push_back(id);
    }
    std::sort(file_ids_.begin(), file_ids_.end());

    // Compute total_size_ for closed files (all except the last, which we'll open).
    total_size_ = 0;

    if (!file_ids_.empty()) {
        // Sum sizes of all files except the last (which becomes active).
        for (size_t i = 0; i + 1 < file_ids_.size(); ++i) {
            struct stat st;
            std::string path = walFilePath(dir, file_ids_[i]);
            if (::stat(path.c_str(), &st) == 0) {
                total_size_ += static_cast<uint64_t>(st.st_size);
            }
        }
        // Open the last file for appending.
        current_file_id_ = file_ids_.back();
        Status s = wal_.open(walFilePath(dir, current_file_id_));
        if (!s.ok()) return s;
    } else {
        // No files exist yet — allocate the first one.
        uint32_t id;
        Status s = allocateFileId(id);
        if (!s.ok()) return s;
        current_file_id_ = id;
        file_ids_.push_back(id);

        // Register in Manifest.
        s = manifest_->set(file_prefix_ + std::to_string(id), "");
        if (!s.ok()) return s;

        s = wal_.create(walFilePath(dir, id));
        if (!s.ok()) return s;
    }

    return Status::OK();
}

Status GlobalIndexWAL::close() {
    wal_.abort();
    return wal_.close();
}

// --- Batch recording (in-memory, no I/O) ---

void GlobalIndexWAL::appendPut(std::string_view key, uint64_t packed_version,
                                uint32_t segment_id) {
    if (!wal_.isOpen()) return;

    // Domain payload: [op:1][packed_version:8][segment_id:4][key_len:2][key:var]
    size_t payload_len = 1 + 8 + 4 + 2 + key.size();
    uint8_t buf[256];
    std::vector<uint8_t> heap_buf;
    uint8_t* p;
    if (payload_len <= sizeof(buf)) {
        p = buf;
    } else {
        heap_buf.resize(payload_len);
        p = heap_buf.data();
    }

    *p++ = static_cast<uint8_t>(WalOp::kPut);
    std::memcpy(p, &packed_version, 8); p += 8;
    std::memcpy(p, &segment_id, 4); p += 4;
    uint16_t key_len = static_cast<uint16_t>(key.size());
    std::memcpy(p, &key_len, 2); p += 2;
    std::memcpy(p, key.data(), key.size());

    uint8_t* base = (heap_buf.empty()) ? buf : heap_buf.data();
    wal_.put(base, payload_len);
    ++entry_count_;
}

void GlobalIndexWAL::appendRelocate(std::string_view key, uint64_t packed_version,
                                     uint32_t old_segment_id, uint32_t new_segment_id) {
    if (!wal_.isOpen()) return;

    // Domain payload: [op:1][packed_version:8][old_seg:4][new_seg:4][key_len:2][key:var]
    size_t payload_len = 1 + 8 + 4 + 4 + 2 + key.size();
    uint8_t buf[256];
    std::vector<uint8_t> heap_buf;
    uint8_t* p;
    if (payload_len <= sizeof(buf)) {
        p = buf;
    } else {
        heap_buf.resize(payload_len);
        p = heap_buf.data();
    }

    *p++ = static_cast<uint8_t>(WalOp::kRelocate);
    std::memcpy(p, &packed_version, 8); p += 8;
    std::memcpy(p, &old_segment_id, 4); p += 4;
    std::memcpy(p, &new_segment_id, 4); p += 4;
    uint16_t key_len = static_cast<uint16_t>(key.size());
    std::memcpy(p, &key_len, 2); p += 2;
    std::memcpy(p, key.data(), key.size());

    uint8_t* base = (heap_buf.empty()) ? buf : heap_buf.data();
    wal_.put(base, payload_len);
    ++entry_count_;
}

void GlobalIndexWAL::appendEliminate(std::string_view key, uint64_t packed_version,
                                      uint32_t segment_id) {
    if (!wal_.isOpen()) return;

    // Domain payload: [op:1][packed_version:8][segment_id:4][key_len:2][key:var]
    size_t payload_len = 1 + 8 + 4 + 2 + key.size();
    uint8_t buf[256];
    std::vector<uint8_t> heap_buf;
    uint8_t* p;
    if (payload_len <= sizeof(buf)) {
        p = buf;
    } else {
        heap_buf.resize(payload_len);
        p = heap_buf.data();
    }

    *p++ = static_cast<uint8_t>(WalOp::kEliminate);
    std::memcpy(p, &packed_version, 8); p += 8;
    std::memcpy(p, &segment_id, 4); p += 4;
    uint16_t key_len = static_cast<uint16_t>(key.size());
    std::memcpy(p, &key_len, 2); p += 2;
    std::memcpy(p, key.data(), key.size());

    uint8_t* base = (heap_buf.empty()) ? buf : heap_buf.data();
    wal_.put(base, payload_len);
    ++entry_count_;
}

Status GlobalIndexWAL::commit(uint64_t version, bool sync) {
    if (!wal_.isOpen()) return Status::OK();

    // Append kCommit domain payload: [op:1][version:8]
    uint8_t commit_buf[9];
    commit_buf[0] = static_cast<uint8_t>(WalOp::kCommit);
    std::memcpy(commit_buf + 1, &version, 8);
    wal_.put(commit_buf, 9);

    Status s = wal_.commit(sync);
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

    Status s = wal_.close();
    if (!s.ok()) return s;

    // Allocate a new file ID.
    uint32_t new_id;
    s = allocateFileId(new_id);
    if (!s.ok()) return s;

    // Register in Manifest.
    s = manifest_->set(file_prefix_ + std::to_string(new_id), "");
    if (!s.ok()) return s;

    // Create the new file.
    current_file_id_ = new_id;
    file_ids_.push_back(new_id);
    return wal_.create(walFilePath(walDir(), new_id));
}

Status GlobalIndexWAL::allocateFileId(uint32_t& file_id) {
    file_id = next_file_id_++;
    return manifest_->set(next_file_id_key_, std::to_string(next_file_id_));
}

std::unique_ptr<WALStream> GlobalIndexWAL::replayStream() const {
    return stream::walReplay(walDir(), file_ids_);
}

Status GlobalIndexWAL::truncate() {
    entry_count_ = 0;
    total_size_ = 0;
    wal_.abort();
    return Status::OK();
}

uint64_t GlobalIndexWAL::size() const {
    return total_size_ + wal_.size();
}

std::string GlobalIndexWAL::walDir() const {
    return db_path_ + "/wal/" + name_;
}

std::string GlobalIndexWAL::walFilePath(const std::string& wal_dir, uint32_t file_id) {
    char buf[16];
    std::snprintf(buf, sizeof(buf), "%08u", file_id);
    return wal_dir + "/" + buf + ".log";
}

}  // namespace internal
}  // namespace kvlite
