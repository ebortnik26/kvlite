#include "internal/storage_manager.h"

#include <filesystem>
#include <set>
#include <string>

#include "internal/manifest.h"

namespace kvlite {
namespace internal {

static constexpr char kNextSegmentIdKey[] = "next_segment_id";
static constexpr char kSegmentPrefix[] = "segment.";

StorageManager::StorageManager(Manifest& manifest) : manifest_(manifest) {}
StorageManager::~StorageManager() = default;

Status StorageManager::open(const std::string& db_path) {
    return open(db_path, Options{});
}

Status StorageManager::open(const std::string& db_path,
                            const Options& options) {
    db_path_ = db_path;
    options_ = options;
    is_open_ = true;

    Status s = recover();
    if (!s.ok()) {
        is_open_ = false;
        return s;
    }
    return Status::OK();
}

Status StorageManager::recover() {
    // Read next_segment_id from manifest.
    std::string val;
    if (manifest_.get(kNextSegmentIdKey, val)) {
        next_segment_id_.store(static_cast<uint32_t>(std::stoul(val)),
                               std::memory_order_relaxed);
    }

    // Reopen segments listed in manifest.
    std::set<std::string> tracked_files;
    auto keys = manifest_.getKeysWithPrefix(kSegmentPrefix);
    for (const auto& key : keys) {
        // Parse segment ID from "segment.<id>".
        std::string id_str = key.substr(std::strlen(kSegmentPrefix));
        uint32_t id = static_cast<uint32_t>(std::stoul(id_str));

        std::string path = segmentPath(id);
        tracked_files.insert(std::filesystem::path(path).filename().string());

        Segment seg;
        Status s = seg.open(path);
        if (!s.ok()) {
            return Status::IOError("Failed to reopen segment " +
                                   std::to_string(id) + ": " + s.message());
        }

        std::unique_lock lock(mutex_);
        segments_.emplace(id, std::move(seg));
    }

    // Purge orphan segment files not tracked by manifest.
    if (options_.purge_untracked_files) {
        std::error_code ec;
        for (const auto& entry :
             std::filesystem::directory_iterator(db_path_, ec)) {
            if (!entry.is_regular_file()) continue;
            std::string fname = entry.path().filename().string();
            if (fname.rfind("segment_", 0) == 0 &&
                fname.size() > 5 &&
                fname.substr(fname.size() - 5) == ".data" &&
                tracked_files.find(fname) == tracked_files.end()) {
                std::filesystem::remove(entry.path());
            }
        }
    }

    return Status::OK();
}

Status StorageManager::close() {
    std::unique_lock lock(mutex_);
    for (auto& [id, seg] : segments_) {
        seg.close();
    }
    segments_.clear();
    is_open_ = false;
    return Status::OK();
}

bool StorageManager::isOpen() const {
    return is_open_;
}

Status StorageManager::createSegment(uint32_t id) {
    std::string path = segmentPath(id);
    Segment seg;
    Status s = seg.create(path, id);
    if (!s.ok()) return s;

    std::unique_lock lock(mutex_);
    segments_.emplace(id, std::move(seg));
    lock.unlock();

    manifest_.set(std::string(kSegmentPrefix) + std::to_string(id), "");
    return Status::OK();
}

Status StorageManager::removeSegment(uint32_t id) {
    std::string path = segmentPath(id);

    std::unique_lock lock(mutex_);
    auto it = segments_.find(id);
    if (it != segments_.end()) {
        it->second.close();
        segments_.erase(it);
    }
    lock.unlock();

    manifest_.remove(std::string(kSegmentPrefix) + std::to_string(id));

    std::error_code ec;
    std::filesystem::remove(path, ec);
    if (ec) {
        return Status::IOError("Failed to delete segment file: " + ec.message());
    }
    return Status::OK();
}

Segment* StorageManager::getSegment(uint32_t id) {
    std::shared_lock lock(mutex_);
    auto it = segments_.find(id);
    return it != segments_.end() ? &it->second : nullptr;
}

const Segment* StorageManager::getSegment(uint32_t id) const {
    std::shared_lock lock(mutex_);
    auto it = segments_.find(id);
    return it != segments_.end() ? &it->second : nullptr;
}

std::vector<uint32_t> StorageManager::getSegmentIds() const {
    std::shared_lock lock(mutex_);
    std::vector<uint32_t> ids;
    ids.reserve(segments_.size());
    for (const auto& [id, seg] : segments_) {
        ids.push_back(id);
    }
    return ids;
}

size_t StorageManager::segmentCount() const {
    std::shared_lock lock(mutex_);
    return segments_.size();
}

uint64_t StorageManager::totalDataSize() const {
    std::shared_lock lock(mutex_);
    uint64_t total = 0;
    for (const auto& [id, seg] : segments_) {
        total += seg.dataSize();
    }
    return total;
}

uint32_t StorageManager::allocateSegmentId() {
    uint32_t id = next_segment_id_.fetch_add(1, std::memory_order_relaxed);
    manifest_.set(kNextSegmentIdKey, std::to_string(id + 1));
    return id;
}

std::string StorageManager::segmentPath(uint32_t segment_id) const {
    return db_path_ + "/segment_" + std::to_string(segment_id) + ".data";
}

}  // namespace internal
}  // namespace kvlite
