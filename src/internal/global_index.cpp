#include "internal/global_index.h"

#include <algorithm>
#include <cstring>
#include <filesystem>

#include "internal/global_index_savepoint.h"
#include "internal/global_index_wal.h"
#include "internal/manifest.h"
#include "internal/wal_stream.h"

namespace kvlite {
namespace internal {

GlobalIndex::GlobalIndex(Manifest& manifest)
    : manifest_(manifest),
      wal_(std::make_unique<GlobalIndexWAL>()) {}

GlobalIndex::~GlobalIndex() {
    if (is_open_) {
        close();
    }
}

// --- Lifecycle ---

Status GlobalIndex::open(const std::string& db_path, const Options& options) {
    if (is_open_) {
        return Status::InvalidArgument("Already open");
    }
    db_path_ = db_path;
    options_ = options;

    GlobalIndexWAL::Options wal_opts;
    Status s = wal_->open(db_path, manifest_, wal_opts);
    if (!s.ok()) return s;

    // Load persisted max_version from last savepoint (if any).
    std::string mv_str;
    if (manifest_.get(ManifestKey::kGiSavepointMaxVersion, mv_str) && !mv_str.empty()) {
        max_version_.store(std::stoull(mv_str), std::memory_order_relaxed);
    }

    s = recover();
    if (!s.ok()) {
        wal_->close();
        return s;
    }

    is_open_ = true;
    return Status::OK();
}

Status GlobalIndex::recover() {
    // Re-truncate WAL: delete files with max_version <= savepoint max_version.
    // Idempotent — safe if the previous truncation partially completed.
    Status s = wal_->truncate(max_version_.load(std::memory_order_relaxed));
    if (!s.ok()) return s;

    std::string valid_dir = savepointDir();

    s = recoverPartialSwap(valid_dir);
    if (!s.ok()) return s;

    // Load savepoint if one exists.
    namespace fs = std::filesystem;
    std::error_code ec;
    if (fs::exists(valid_dir, ec) && fs::is_directory(valid_dir, ec)) {
        s = savepoint::load(valid_dir, dht_, key_count_, options_.savepoint_threads);
        if (!s.ok()) return s;
    }

    s = replayWAL();
    if (!s.ok()) return s;

    return writeConvergenceSavepoint(valid_dir);
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

Status GlobalIndex::replayWAL() {
    // Determine the valid segment range from the Manifest.
    // WAL entries referencing segments beyond max_seg_id are orphans
    // from a flush that crashed before the Manifest commit point.
    std::string seg_val;
    bool has_max_seg = manifest_.get(ManifestKey::kSegmentsMaxSegId, seg_val);
    uint32_t max_seg_id = has_max_seg
        ? static_cast<uint32_t>(std::stoul(seg_val)) : 0;

    // Replay all remaining WAL records, ordered by file (= by max_version).
    // Skip entries that reference unregistered segments.
    auto stream = wal_->replayStream();
    while (stream->valid()) {
        const auto& rec = stream->record();
        switch (rec.op) {
            case WalOp::kPut:
                if (!has_max_seg || rec.segment_id > max_seg_id) break;
                applyPut(rec.hkey, rec.packed_version, rec.segment_id);
                {
                    uint64_t cur = max_version_.load(std::memory_order_relaxed);
                    if (rec.packed_version > cur)
                        max_version_.store(rec.packed_version, std::memory_order_relaxed);
                }
                break;
            case WalOp::kRelocate:
                if (!has_max_seg || rec.new_segment_id > max_seg_id) break;
                applyRelocate(rec.hkey, rec.packed_version,
                              rec.segment_id, rec.new_segment_id);
                break;
            case WalOp::kEliminate:
                applyEliminate(rec.hkey, rec.packed_version, rec.segment_id);
                break;
        }
        Status s = stream->next();
        if (!s.ok()) return s;
    }
    return Status::OK();
}

Status GlobalIndex::writeConvergenceSavepoint(const std::string& valid_dir) {
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

    s = manifest_.set(ManifestKey::kGiSavepointMaxVersion,
                      std::to_string(max_version_.load(std::memory_order_relaxed)));
    if (!s.ok()) return s;

    s = wal_->truncate(max_version_.load(std::memory_order_relaxed));
    if (!s.ok()) return s;

    // Start a fresh WAL file.
    return wal_->startNewFile();
}

Status GlobalIndex::close() {
    if (!is_open_) {
        return Status::OK();
    }
    Status s = storeSavepoint(max_version_.load(std::memory_order_relaxed));
    if (!s.ok()) {
        wal_->close();
        is_open_ = false;
        return s;
    }
    wal_->close();
    is_open_ = false;
    return Status::OK();
}

bool GlobalIndex::isOpen() const {
    return is_open_;
}

// --- Index Operations ---

Status GlobalIndex::put(uint64_t hkey, uint64_t packed_version, uint32_t segment_id) {
    std::shared_lock<std::shared_mutex> lock(savepoint_mu_);
    Status s = wal_->appendPut(hkey, packed_version, segment_id, WalProducer::kWB);
    if (!s.ok()) return s;
    applyPut(hkey, packed_version, segment_id);
    uint64_t cur = max_version_.load(std::memory_order_relaxed);
    while (packed_version > cur &&
           !max_version_.compare_exchange_weak(cur, packed_version,
               std::memory_order_relaxed)) {}
    wal_->updateMaxVersion(packed_version);
    return Status::OK();
}

Status GlobalIndex::stagePut(uint64_t hkey, uint64_t packed_version, uint32_t segment_id) {
    applyPut(hkey, packed_version, segment_id);  // DHT first
    Status s = wal_->stagePut(hkey, packed_version, segment_id, WalProducer::kWB);
    if (!s.ok()) return s;
    uint64_t cur = max_version_.load(std::memory_order_relaxed);
    while (packed_version > cur &&
           !max_version_.compare_exchange_weak(cur, packed_version,
               std::memory_order_relaxed)) {}
    wal_->updateMaxVersion(packed_version);
    return Status::OK();
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

Status GlobalIndex::relocate(uint64_t hkey, uint64_t packed_version,
                              uint32_t old_segment_id, uint32_t new_segment_id) {
    // Caller must hold a BatchGuard.
    Status s = wal_->stageRelocate(hkey, packed_version, old_segment_id, new_segment_id, WalProducer::kGC);
    if (!s.ok()) return s;
    applyRelocate(hkey, packed_version, old_segment_id, new_segment_id);
    return Status::OK();
}

Status GlobalIndex::eliminate(uint64_t hkey, uint64_t packed_version,
                               uint32_t segment_id) {
    // Caller must hold a BatchGuard.
    Status s = wal_->stageEliminate(hkey, packed_version, segment_id, WalProducer::kGC);
    if (!s.ok()) return s;
    applyEliminate(hkey, packed_version, segment_id);
    return Status::OK();
}

// --- Iteration ---

void GlobalIndex::forEachGroup(
    const std::function<void(uint64_t hash,
                             const std::vector<uint64_t>& packed_versions,
                             const std::vector<uint32_t>& segment_ids)>& fn) const {
    // DHT stores (packed_versions, ids=segment_ids) — matches caller's signature
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

// --- WAL commit ---

Status GlobalIndex::commitWB(uint64_t max_version) {
    uint64_t cur = max_version_.load(std::memory_order_relaxed);
    while (max_version > cur &&
           !max_version_.compare_exchange_weak(cur, max_version,
               std::memory_order_relaxed)) {}
    wal_->updateMaxVersion(max_version);
    return wal_->commit(WalProducer::kWB);
}

Status GlobalIndex::commitGC() {
    // Caller must hold a BatchGuard.
    return wal_->commit(WalProducer::kGC);
}

// --- Binary savepoint ---

Status GlobalIndex::storeSavepoint(uint64_t /*savepoint_version*/) {
    namespace fs = std::filesystem;
    std::unique_lock<std::shared_mutex> lock(savepoint_mu_);

    uint64_t snapshot_version = max_version_.load(std::memory_order_relaxed);

    std::string valid_dir = savepointDir();
    std::string tmp_dir = valid_dir + ".tmp";
    std::string old_dir = valid_dir + ".old";

    // Write savepoint to temporary directory.
    Status s = savepoint::store(tmp_dir, dht_,
                                key_count_.load(std::memory_order_relaxed),
                                options_.savepoint_threads);
    if (!s.ok()) return s;

    // Persist snapshot_version to Manifest BEFORE the atomic swap.
    // If we crash after this but before the swap completes, recovery will
    // over-truncate WAL (safe: those records are captured in the new savepoint
    // that will be discarded), load the old savepoint, and replay — correct
    // because WAL replay is idempotent and recovery writes a fresh savepoint.
    s = manifest_.set(ManifestKey::kGiSavepointMaxVersion,
                      std::to_string(snapshot_version));
    if (!s.ok()) return s;

    // Atomic swap: valid → old, tmp → valid, remove old.
    // Recovery handles partial completion (see recover()).
    std::error_code ec;
    if (fs::exists(valid_dir, ec)) {
        fs::remove_all(old_dir, ec);  // clean up any leftover
        fs::rename(valid_dir, old_dir, ec);
        if (ec) {
            return Status::IOError("Failed to move old savepoint: " + ec.message());
        }
    }
    fs::rename(tmp_dir, valid_dir, ec);
    if (ec) {
        // Try to restore old savepoint.
        std::error_code ec2;
        if (fs::exists(old_dir, ec2)) {
            fs::rename(old_dir, valid_dir, ec2);
        }
        return Status::IOError("Failed to install savepoint: " + ec.message());
    }
    fs::remove_all(old_dir, ec);  // best-effort cleanup

    s = wal_->truncateForSavepoint(snapshot_version);
    if (!s.ok()) return s;

    // Start a fresh WAL file so replayed records from the old active
    // file don't duplicate the savepoint data on next recovery.
    return wal_->startNewFile();
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

// --- Private ---

void GlobalIndex::applyPut(uint64_t hkey, uint64_t packed_version,
                           uint32_t segment_id) {
    if (dht_.addEntryIsNew(hkey, packed_version, segment_id)) {
        key_count_.fetch_add(1, std::memory_order_relaxed);
    }
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

Status GlobalIndex::maybeSavepoint() {
    if (wal_->size() == 0) {
        return Status::OK();
    }
    return storeSavepoint(max_version_.load(std::memory_order_relaxed));
}

std::string GlobalIndex::savepointDir() const {
    return db_path_ + "/gi/savepoint";
}

}  // namespace internal
}  // namespace kvlite
