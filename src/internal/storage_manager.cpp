#include "internal/storage_manager.h"

#include <string>

#include "internal/manifest.h"

namespace kvlite {
namespace internal {

static constexpr char kNextSegmentIdKey[] = "next_segment_id";
static constexpr char kSegmentPrefix[] = "segment.";

StorageManager::StorageManager(Manifest& manifest) : manifest_(manifest) {}
StorageManager::~StorageManager() = default;

Status StorageManager::open(const std::string& db_path) {
    db_path_ = db_path;
    is_open_ = true;
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
    auto keys = manifest_.getKeysWithPrefix(kSegmentPrefix);
    for (const auto& key : keys) {
        // Parse segment ID from "segment.<id>".
        std::string id_str = key.substr(std::strlen(kSegmentPrefix));
        uint32_t id = static_cast<uint32_t>(std::stoul(id_str));

        std::string path = segmentPath(id);
        Segment seg;
        Status s = seg.open(path);
        if (!s.ok()) {
            return Status::IOError("Failed to reopen segment " +
                                   std::to_string(id) + ": " + s.message());
        }

        std::unique_lock lock(mutex_);
        segments_.emplace(id, std::move(seg));
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

void StorageManager::addSegment(uint32_t id, Segment& segment) {
    std::unique_lock lock(mutex_);
    segments_.emplace(id, std::move(segment));
    lock.unlock();

    manifest_.set(std::string(kSegmentPrefix) + std::to_string(id), "");
}

void StorageManager::removeSegment(uint32_t id) {
    std::unique_lock lock(mutex_);
    auto it = segments_.find(id);
    if (it != segments_.end()) {
        it->second.close();
        segments_.erase(it);
    }
    lock.unlock();

    manifest_.remove(std::string(kSegmentPrefix) + std::to_string(id));
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
