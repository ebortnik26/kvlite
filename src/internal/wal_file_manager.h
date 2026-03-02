#ifndef KVLITE_INTERNAL_WAL_FILE_MANAGER_H
#define KVLITE_INTERNAL_WAL_FILE_MANAGER_H

#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>

#include "internal/manifest.h"
#include "kvlite/status.h"

namespace kvlite {
namespace internal {

// Manages WAL file IDs, Manifest persistence, and path generation.
//
// Groups the file-tracking state that was spread across GlobalIndexWAL
// into a cohesive class. All methods are short enough to be header-only.
class WALFileManager {
public:
    void open(const std::string& db_path, Manifest& manifest) {
        db_path_ = db_path;
        manifest_ = &manifest;
        loadManifestState();
    }

    uint32_t nextFileId() const {
        return has_files_ ? max_file_id_ + 1 : 0;
    }

    Status persistFileId(uint32_t file_id) {
        if (!has_files_) {
            has_files_ = true;
            min_file_id_ = file_id;
            max_file_id_ = file_id;
            Status s = manifest_->set(ManifestKey::kGiWalMinFileId,
                                      std::to_string(file_id));
            if (!s.ok()) return s;
            return manifest_->set(ManifestKey::kGiWalMaxFileId,
                                  std::to_string(file_id));
        }
        max_file_id_ = file_id;
        return manifest_->set(ManifestKey::kGiWalMaxFileId,
                              std::to_string(file_id));
    }

    std::string walDir() const {
        return db_path_ + "/gi/wal";
    }

    static std::string walFilePath(const std::string& wal_dir, uint32_t file_id) {
        char buf[16];
        std::snprintf(buf, sizeof(buf), "%08u", file_id);
        return wal_dir + "/" + buf + ".log";
    }

    // --- State accessors ---

    uint32_t currentFileId() const { return current_file_id_; }
    const std::vector<uint32_t>& fileIds() const { return file_ids_; }
    uint64_t totalSize() const { return total_size_; }
    bool hasFiles() const { return has_files_; }

    // --- State mutators ---

    void setCurrentFileId(uint32_t id) { current_file_id_ = id; }

    void recordFileCreated(uint32_t id) {
        current_file_id_ = id;
        file_ids_.push_back(id);
    }

    void addClosedFileSize(uint64_t size) {
        total_size_ += size;
    }

    void truncateToActive() {
        std::string dir = walDir();
        for (uint32_t fid : file_ids_) {
            if (fid == current_file_id_) continue;
            std::string path = walFilePath(dir, fid);
            std::remove(path.c_str());
        }
        file_ids_.clear();
        file_ids_.push_back(current_file_id_);
        min_file_id_ = current_file_id_;
        manifest_->set(ManifestKey::kGiWalMinFileId,
                       std::to_string(min_file_id_));
        total_size_ = 0;
    }

private:
    void loadManifestState() {
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

    uint32_t min_file_id_ = 0;
    uint32_t max_file_id_ = 0;
    uint32_t current_file_id_ = 0;
    bool has_files_ = false;
    uint64_t total_size_ = 0;
    std::vector<uint32_t> file_ids_;
    std::string db_path_;
    Manifest* manifest_ = nullptr;
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_WAL_FILE_MANAGER_H
