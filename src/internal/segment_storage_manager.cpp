#include "internal/segment_storage_manager.h"

#include <filesystem>
#include <set>
#include <string>

#include "internal/manifest.h"

namespace kvlite {
namespace internal {

static constexpr char kNextSegmentIdKey[] = "next_segment_id";
static constexpr char kSegmentPrefix[] = "segment.";

SegmentStorageManager::SegmentStorageManager(Manifest& manifest) : manifest_(manifest) {}
SegmentStorageManager::~SegmentStorageManager() = default;

Status SegmentStorageManager::open(const std::string& db_path) {
    return open(db_path, Options{});
}

Status SegmentStorageManager::open(const std::string& db_path,
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

Status SegmentStorageManager::recover() {
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

Status SegmentStorageManager::close() {
    std::unique_lock lock(mutex_);
    for (auto& [id, seg] : segments_) {
        seg.close();
    }
    segments_.clear();
    is_open_ = false;
    return Status::OK();
}

bool SegmentStorageManager::isOpen() const {
    return is_open_;
}

Status SegmentStorageManager::createSegment(uint32_t id) {
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

Status SegmentStorageManager::removeSegment(uint32_t id) {
    std::unique_lock lock(mutex_);

    // If pinned, defer removal.
    auto pc = pin_counts_.find(id);
    if (pc != pin_counts_.end() && pc->second > 0) {
        deferred_removals_.insert(id);
        return Status::OK();
    }

    // Proceed with actual removal.
    auto it = segments_.find(id);
    if (it != segments_.end()) {
        it->second.close();
        segments_.erase(it);
    }
    deferred_removals_.erase(id);
    lock.unlock();

    std::string path = segmentPath(id);
    manifest_.remove(std::string(kSegmentPrefix) + std::to_string(id));

    std::error_code ec;
    std::filesystem::remove(path, ec);
    if (ec) {
        return Status::IOError("Failed to delete segment file: " + ec.message());
    }
    return Status::OK();
}

void SegmentStorageManager::pinSegment(uint32_t id) {
    std::unique_lock lock(mutex_);
    pin_counts_[id]++;
}

void SegmentStorageManager::unpinSegment(uint32_t id) {
    std::unique_lock lock(mutex_);
    auto it = pin_counts_.find(id);
    if (it == pin_counts_.end()) return;
    if (--it->second == 0) {
        pin_counts_.erase(it);
        if (deferred_removals_.count(id)) {
            lock.unlock();
            removeSegment(id);
        }
    }
}

Segment* SegmentStorageManager::getSegment(uint32_t id) {
    std::shared_lock lock(mutex_);
    auto it = segments_.find(id);
    return it != segments_.end() ? &it->second : nullptr;
}

const Segment* SegmentStorageManager::getSegment(uint32_t id) const {
    std::shared_lock lock(mutex_);
    auto it = segments_.find(id);
    return it != segments_.end() ? &it->second : nullptr;
}

std::vector<uint32_t> SegmentStorageManager::getSegmentIds() const {
    std::shared_lock lock(mutex_);
    std::vector<uint32_t> ids;
    ids.reserve(segments_.size());
    for (const auto& [id, seg] : segments_) {
        ids.push_back(id);
    }
    return ids;
}

size_t SegmentStorageManager::segmentCount() const {
    std::shared_lock lock(mutex_);
    return segments_.size();
}

uint64_t SegmentStorageManager::totalDataSize() const {
    std::shared_lock lock(mutex_);
    uint64_t total = 0;
    for (const auto& [id, seg] : segments_) {
        total += seg.dataSize();
    }
    return total;
}

uint32_t SegmentStorageManager::allocateSegmentId() {
    uint32_t id = next_segment_id_.fetch_add(1, std::memory_order_relaxed);
    manifest_.set(kNextSegmentIdKey, std::to_string(id + 1));
    return id;
}

std::string SegmentStorageManager::segmentPath(uint32_t segment_id) const {
    return db_path_ + "/segment_" + std::to_string(segment_id) + ".data";
}

}  // namespace internal
}  // namespace kvlite
