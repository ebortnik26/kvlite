#include "internal/storage_manager.h"


namespace kvlite {
namespace internal {

StorageManager::StorageManager() = default;
StorageManager::~StorageManager() = default;

Status StorageManager::open(const std::string& db_path) {
    db_path_ = db_path;
    is_open_ = true;
    return Status::OK();
}

Status StorageManager::recover() {
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
}

void StorageManager::removeSegment(uint32_t id) {
    std::unique_lock lock(mutex_);
    auto it = segments_.find(id);
    if (it != segments_.end()) {
        it->second.close();
        segments_.erase(it);
    }
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
    return next_segment_id_.fetch_add(1, std::memory_order_relaxed);
}

std::string StorageManager::segmentPath(uint32_t segment_id) const {
    return db_path_ + "/segment_" + std::to_string(segment_id) + ".data";
}

}  // namespace internal
}  // namespace kvlite
