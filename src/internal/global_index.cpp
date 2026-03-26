#include "internal/global_index.h"

#include <filesystem>

#include "internal/global_index_savepoint.h"
#include "internal/manifest.h"
#include "internal/segment.h"
#include "internal/segment_storage_manager.h"

namespace kvlite {
namespace internal {

GlobalIndex::GlobalIndex(Manifest& manifest)
    : manifest_(manifest) {}

GlobalIndex::~GlobalIndex() {
    if (is_open_) {
        close();
    }
}

// --- Lifecycle ---

Status GlobalIndex::open(const std::string& db_path, const Options& options,
                          SegmentStorageManager& storage) {
    if (is_open_) {
        return Status::InvalidArgument("Already open");
    }
    db_path_ = db_path;
    options_ = options;

    // Create gi/ subdirectory if needed (for savepoint).
    std::error_code ec;
    std::filesystem::create_directories(db_path + "/gi", ec);

    // Load persisted max_segment_id from last savepoint (if any).
    std::string val;
    if (manifest_.get(ManifestKey::kGiSavepointMaxSegmentId, val) && !val.empty()) {
        savepoint_max_segment_id_ = static_cast<uint32_t>(std::stoul(val));
    }

    Status s = recover(storage);
    if (!s.ok()) return s;

    is_open_ = true;
    return Status::OK();
}

Status GlobalIndex::recover(SegmentStorageManager& storage) {
    std::string valid_dir = savepointDir();

    Status s = recoverPartialSwap(valid_dir);
    if (!s.ok()) return s;

    // Load savepoint if one exists.
    namespace fs = std::filesystem;
    std::error_code ec;
    if (fs::exists(valid_dir, ec) && fs::is_directory(valid_dir, ec)) {
        s = savepoint::load(valid_dir, dht_, key_count_, options_.savepoint_threads);
        if (!s.ok()) return s;
    }

    // Replay lineage from segments created after the savepoint.
    s = replaySegmentLineages(storage, savepoint_max_segment_id_);
    if (!s.ok()) return s;

    // Determine the current max segment ID for the convergence savepoint.
    auto seg_ids = storage.getSegmentIds();
    uint32_t max_seg_id = seg_ids.empty() ? savepoint_max_segment_id_ : seg_ids.back();

    return writeConvergenceSavepoint(valid_dir, max_seg_id);
}

Status GlobalIndex::recoverPartialSwap(const std::string& valid_dir) {
    namespace fs = std::filesystem;
    std::string old_dir = valid_dir + ".old";
    std::string tmp_dir = valid_dir + ".tmp";
    std::error_code ec;

    // Discard incomplete write.
    fs::remove_all(tmp_dir, ec);

    // If .old exists but valid dir doesn't, the swap was interrupted — restore.
    if (!fs::exists(valid_dir, ec) && fs::exists(old_dir, ec)) {
        fs::rename(old_dir, valid_dir, ec);
    }

    // Clean up leftover.
    fs::remove_all(old_dir, ec);
    return Status::OK();
}

Status GlobalIndex::replaySegmentLineages(SegmentStorageManager& storage,
                                           uint32_t after_segment_id) {
    auto seg_ids = storage.getSegmentIds();

    // Replay in ascending segment_id order for correctness:
    // flush segments' puts are applied first, then GC segments' relocations
    // and eliminations supersede them.
    for (uint32_t id : seg_ids) {
        if (id <= after_segment_id) continue;

        const Segment* seg = storage.getSegment(id);
        if (!seg || seg->state() != Segment::State::kReadable) continue;

        Lineage lineage;
        Status s = seg->readLineage(lineage);
        if (!s.ok()) return s;

        // Apply put/relocation entries.
        for (const auto& e : lineage.entries) {
            applyPut(e.hkey, e.packed_version, id);
        }

        // Apply eliminations (GC segments only).
        for (const auto& e : lineage.eliminations) {
            applyEliminate(e.hkey, e.packed_version, e.old_segment_id);
        }
    }
    return Status::OK();
}

Status GlobalIndex::writeConvergenceSavepoint(const std::string& valid_dir,
                                               uint32_t max_seg_id) {
    namespace fs = std::filesystem;
    std::string tmp_dir = valid_dir + ".tmp";
    std::string old_dir = valid_dir + ".old";
    std::error_code ec;

    // Write a fresh savepoint capturing the fully recovered state.
    Status s = savepoint::store(tmp_dir, dht_,
                         key_count_.load(std::memory_order_relaxed),
                         options_.savepoint_threads);
    if (!s.ok()) return s;

    // Atomic swap: valid → old, tmp → valid, remove old.
    fs::remove_all(old_dir, ec);
    if (fs::exists(valid_dir, ec)) {
        fs::rename(valid_dir, old_dir, ec);
    }
    fs::rename(tmp_dir, valid_dir, ec);
    fs::remove_all(old_dir, ec);

    savepoint_max_segment_id_ = max_seg_id;
    s = manifest_.set(ManifestKey::kGiSavepointMaxSegmentId,
                      std::to_string(max_seg_id));
    return s;
}

Status GlobalIndex::close() {
    if (!is_open_) {
        return Status::OK();
    }
    // Savepoint is written by DB::close() with the current max segment ID
    // before calling this method. Nothing else to clean up.
    is_open_ = false;
    return Status::OK();
}

bool GlobalIndex::isOpen() const {
    return is_open_;
}

// --- Index Operations ---

void GlobalIndex::applyPut(uint64_t hkey, uint64_t packed_version, uint32_t segment_id) {
    if (dht_.addEntryIsNew(hkey, packed_version, segment_id)) {
        key_count_.fetch_add(1, std::memory_order_relaxed);
    }
}

bool GlobalIndex::get(uint64_t hkey,
                  std::vector<uint32_t>& segment_ids,
                  std::vector<uint64_t>& packed_versions) const {
    std::vector<uint64_t> pvs;
    std::vector<uint32_t> ids;
    if (!dht_.findAll(hkey, pvs, ids)) return false;
    packed_versions = std::move(pvs);
    segment_ids = std::move(ids);
    return true;
}

bool GlobalIndex::get(uint64_t hkey, uint64_t upper_bound,
                  uint64_t& packed_version, uint32_t& segment_id) const {
    std::vector<uint64_t> pvs;
    std::vector<uint32_t> ids;
    if (!dht_.findAll(hkey, pvs, ids)) return false;
    // Entries are sorted by packed_version descending; find the first <= upper_bound.
    for (size_t i = 0; i < pvs.size(); ++i) {
        if (pvs[i] <= upper_bound) {
            packed_version = pvs[i];
            segment_id = ids[i];
            return true;
        }
    }
    return false;
}

Status GlobalIndex::getLatest(uint64_t hkey,
                        uint64_t& packed_version, uint32_t& segment_id) const {
    uint64_t pv;
    uint32_t id;
    if (!dht_.findFirst(hkey, pv, id)) {
        return Status::NotFound("key");
    }
    packed_version = pv;
    segment_id = id;
    return Status::OK();
}

bool GlobalIndex::contains(uint64_t hkey) const {
    return dht_.contains(hkey);
}

void GlobalIndex::applyRelocate(uint64_t hkey, uint64_t packed_version,
                                 uint32_t old_segment_id, uint32_t new_segment_id) {
    dht_.updateEntryId(hkey, packed_version, old_segment_id, new_segment_id);
}

void GlobalIndex::applyEliminate(uint64_t hkey, uint64_t packed_version,
                                  uint32_t segment_id) {
    if (dht_.removeEntry(hkey, packed_version, segment_id)) {
        key_count_.fetch_sub(1, std::memory_order_relaxed);
    }
}

// --- Iteration ---

void GlobalIndex::forEachGroup(
    const std::function<void(uint64_t hash,
                             const std::vector<uint64_t>& packed_versions,
                             const std::vector<uint32_t>& segment_ids)>& fn) const {
    dht_.forEachGroup([&fn](uint64_t hash,
                            const std::vector<uint64_t>& pvs,
                            const std::vector<uint32_t>& ids) {
        fn(hash, pvs, ids);
    });
}

void GlobalIndex::clear() {
    dht_.clear();
    key_count_.store(0, std::memory_order_relaxed);
}

// --- Binary savepoint ---

Status GlobalIndex::storeSavepoint(uint32_t max_segment_id) {
    namespace fs = std::filesystem;
    std::unique_lock<std::shared_mutex> lock(savepoint_mu_);

    std::string valid_dir = savepointDir();
    std::string tmp_dir = valid_dir + ".tmp";
    std::string old_dir = valid_dir + ".old";

    // Write savepoint to temporary directory.
    Status s = savepoint::store(tmp_dir, dht_,
                                key_count_.load(std::memory_order_relaxed),
                                options_.savepoint_threads);
    if (!s.ok()) return s;

    // Persist max_segment_id to Manifest BEFORE the atomic swap.
    s = manifest_.set(ManifestKey::kGiSavepointMaxSegmentId,
                      std::to_string(max_segment_id));
    if (!s.ok()) return s;

    // Atomic swap: valid → old, tmp → valid, remove old.
    std::error_code ec;
    if (fs::exists(valid_dir, ec)) {
        fs::remove_all(old_dir, ec);
        fs::rename(valid_dir, old_dir, ec);
        if (ec) {
            return Status::IOError("Failed to move old savepoint: " + ec.message());
        }
    }
    fs::rename(tmp_dir, valid_dir, ec);
    if (ec) {
        std::error_code ec2;
        if (fs::exists(old_dir, ec2)) {
            fs::rename(old_dir, valid_dir, ec2);
        }
        return Status::IOError("Failed to install savepoint: " + ec.message());
    }
    fs::remove_all(old_dir, ec);

    savepoint_max_segment_id_ = max_segment_id;
    return Status::OK();
}

// --- Statistics ---

size_t GlobalIndex::keyCount() const {
    return key_count_.load(std::memory_order_relaxed);
}

size_t GlobalIndex::entryCount() const {
    return dht_.size();
}

size_t GlobalIndex::memoryUsage() const {
    return dht_.memoryUsage();
}

double GlobalIndex::estimateDeadRatio() const {
    size_t total_groups = 0;
    size_t multi_version_groups = 0;
    forEachGroup([&](uint64_t, const auto& pvs, const auto&) {
        ++total_groups;
        if (pvs.size() > 1) ++multi_version_groups;
    });
    if (total_groups == 0) return 0.0;
    return static_cast<double>(multi_version_groups) /
           static_cast<double>(total_groups);
}

Status GlobalIndex::maybeSavepoint(uint32_t current_max_segment_id) {
    if (current_max_segment_id <= savepoint_max_segment_id_) {
        return Status::OK();
    }
    return storeSavepoint(current_max_segment_id);
}

std::string GlobalIndex::savepointDir() const {
    return db_path_ + "/gi/savepoint";
}

}  // namespace internal
}  // namespace kvlite
