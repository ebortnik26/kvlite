#include "internal/segment_storage_manager.h"

#include <algorithm>
#include <filesystem>
#include <set>
#include <string>

#include "internal/manifest.h"

namespace kvlite {
namespace internal {

using MK = ManifestKey;

SegmentStorageManager::SegmentStorageManager(Manifest& manifest, bool buffered_writes,
                                               uint16_t num_partitions)
    : manifest_(manifest), buffered_writes_(buffered_writes),
      num_partitions_(num_partitions) {}
SegmentStorageManager::~SegmentStorageManager() = default;

Status SegmentStorageManager::open(const std::string& db_path) {
    db_path_ = db_path;
    is_open_ = true;

    // Create flush pool for parallel partition sealing (K > 1).
    if (num_partitions_ > 1) {
        flush_pool_ = std::make_unique<FlushPool>(num_partitions_);
    }

    std::error_code ec;
    std::filesystem::create_directories(segmentsDir(), ec);

    Status s = recover();
    if (!s.ok()) {
        is_open_ = false;
        return s;
    }
    return Status::OK();
}

Status SegmentStorageManager::recover() {
    // Read segment ID range from manifest.
    std::string val;
    uint32_t min_id = 0, max_id = 0;
    bool has_range = false;
    if (manifest_.get(MK::kSegmentsMinSegId, val)) {
        min_id = static_cast<uint32_t>(std::stoul(val));
        has_range = true;
    }
    if (manifest_.get(MK::kSegmentsMaxSegId, val)) {
        max_id = static_cast<uint32_t>(std::stoul(val));
        has_range = true;
    }

    if (has_range) {
        next_segment_id_.store(max_id + 1, std::memory_order_relaxed);
    }

    // Build set of tracked file names for orphan detection.
    // Each segment ID maps to partition files: segment_<id>_<p>.data
    std::set<std::string> tracked_files;
    for (uint32_t id = min_id; has_range && id <= max_id; ++id) {
        std::string base = segmentBasePath(id);

        // Check if partition 0 exists to determine if this segment is present.
        std::string p0 = Segment::partitionPath(base, 0);
        if (!std::filesystem::exists(p0)) {
            // Gap — segment was removed. Track nothing.
            continue;
        }

        Segment seg;
        Status s = seg.open(base);
        if (!s.ok()) {
            // Corrupt or incomplete segment — skip it.
            continue;
        }

        // Track all partition files for this segment.
        for (uint16_t p = 0; p < seg.numPartitions(); ++p) {
            tracked_files.insert(
                std::filesystem::path(Segment::partitionPath(base, p)).filename().string());
        }

        std::unique_lock lock(mutex_);
        segments_.emplace(id, std::move(seg));
    }

    // Purge orphan segment files not tracked by manifest.
    {
        std::error_code ec;
        for (const auto& entry :
             std::filesystem::directory_iterator(segmentsDir(), ec)) {
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
    std::string base = segmentBasePath(id);
    Segment seg;
    Status s = seg.create(base, id, num_partitions_, buffered_writes_);
    if (!s.ok()) return s;

    std::unique_lock lock(mutex_);
    segments_.emplace(id, std::move(seg));
    lock.unlock();

    manifest_.set(MK::kSegmentsMaxSegId, std::to_string(id));
    std::string val;
    if (!manifest_.get(MK::kSegmentsMinSegId, val)) {
        manifest_.set(MK::kSegmentsMinSegId, std::to_string(id));
    }
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

    // Get partition count before erasing.
    uint16_t num_parts = 1;
    auto it = segments_.find(id);
    if (it != segments_.end()) {
        num_parts = it->second.numPartitions();
        it->second.close();
        segments_.erase(it);
    }
    deferred_removals_.erase(id);
    lock.unlock();

    std::string base = segmentBasePath(id);

    // Advance min_segment_id if we're removing the lowest segment.
    std::string val;
    if (manifest_.get(MK::kSegmentsMinSegId, val)) {
        uint32_t min_id = static_cast<uint32_t>(std::stoul(val));
        if (id == min_id) {
            manifest_.set(MK::kSegmentsMinSegId, std::to_string(min_id + 1));
        }
    }

    // Delete all partition files.
    std::error_code ec;
    for (uint16_t p = 0; p < num_parts; ++p) {
        std::filesystem::remove(Segment::partitionPath(base, p), ec);
    }
    return Status::OK();
}

void SegmentStorageManager::registerSegmentId(uint32_t id) {
    manifest_.set(MK::kSegmentsMaxSegId, std::to_string(id));
    std::string val;
    if (!manifest_.get(MK::kSegmentsMinSegId, val)) {
        manifest_.set(MK::kSegmentsMinSegId, std::to_string(id));
    }
}

void SegmentStorageManager::adoptSegment(uint32_t id, Segment seg) {
    std::unique_lock lock(mutex_);
    segments_.emplace(id, std::move(seg));
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
    return next_segment_id_.fetch_add(1, std::memory_order_relaxed);
}

std::string SegmentStorageManager::segmentBasePath(uint32_t segment_id) const {
    return segmentsDir() + "/segment_" + std::to_string(segment_id);
}

std::string SegmentStorageManager::segmentsDir() const {
    return db_path_ + "/segments";
}

}  // namespace internal
}  // namespace kvlite
