#include "kvlite/db.h"

#include <algorithm>
#include <cassert>
#include <memory>
#include <vector>

#include "internal/delta_hash_table.h"
#include "internal/gc.h"
#include "internal/manifest.h"
#include "internal/version_manager.h"
#include "internal/global_index.h"
#include "internal/segment_storage_manager.h"
#include "internal/write_buffer.h"
#include "internal/memtable.h"
#include "internal/log_entry.h"
#include "internal/segment.h"

namespace kvlite {

// --- DB Implementation ---

DB::DB() = default;
DB::~DB() {
    if (isOpen()) {
        close();
    }
}

Status DB::open(const std::string& path, const Options& options) {
    if (is_open_) {
        return Status::InvalidArgument("Database already open");
    }

    db_path_ = path;
    options_ = options;

    // Initialize manifest
    manifest_ = std::make_unique<internal::Manifest>();
    Status s = manifest_->open(path);
    if (!s.ok()) {
        if (s.isNotFound() && options.create_if_missing) {
            s = manifest_->create(path);
        }
        if (!s.ok()) {
            manifest_.reset();
            return s;
        }
    }

    // Initialize version manager
    versions_ = std::make_unique<internal::VersionManager>(*manifest_);
    internal::VersionManager::Options ver_opts;

    s = versions_->open(ver_opts);
    if (!s.ok()) {
        manifest_->close();
        manifest_.reset();
        versions_.reset();
        return s;
    }

    s = versions_->recover();
    if (!s.ok()) {
        versions_->close();
        versions_.reset();
        manifest_->close();
        manifest_.reset();
        return s;
    }

    // Initialize GlobalIndex
    global_index_ = std::make_unique<internal::GlobalIndex>(*manifest_);
    internal::GlobalIndex::Options global_index_opts;
    global_index_opts.sync_writes = options.sync_writes;

    s = global_index_->open(path, global_index_opts);
    if (!s.ok()) {
        versions_->close();
        versions_.reset();
        manifest_->close();
        manifest_.reset();
        global_index_.reset();
        return s;
    }

    // Initialize storage manager
    storage_ = std::make_unique<internal::SegmentStorageManager>(*manifest_);

    s = storage_->open(path);
    if (!s.ok()) {
        versions_->close();
        global_index_->close();
        versions_.reset();
        global_index_.reset();
        storage_.reset();
        manifest_->close();
        manifest_.reset();
        return s;
    }

    // Initialize write buffer with flush callback.
    internal::WriteBuffer::Options wb_opts;
    wb_opts.memtable_size = options.write_buffer_size;
    wb_opts.flush_depth = options.flush_depth;

    auto flush_fn = [this](internal::Memtable& mt) -> Status {
        uint32_t seg_id = storage_->allocateSegmentId();

        Status s = storage_->createSegment(seg_id, false);
        if (!s.ok()) return s;

        auto* seg = storage_->getSegment(seg_id);
        auto result = mt.flush(*seg, versions_->snapshotVersions());
        if (!result.status.ok()) return result.status;

        // Hold the savepoint lock for the entire GI batch (stage + commit).
        {
            internal::GlobalIndex::BatchGuard guard(*global_index_);

            for (const auto& e : result.entries) {
                s = global_index_->stagePut(e.hkey, e.packed_ver, seg_id);
                if (!s.ok()) return s;
            }

            s = global_index_->commitWB(result.max_version);
            if (!s.ok()) return s;
        }

        // Manifest update is the commit point — makes the flush visible.
        s = storage_->registerSegments({seg_id});
        return s;
    };

    write_buffer_ = std::make_unique<internal::WriteBuffer>(wb_opts, flush_fn);

    is_open_ = true;

    startGCLoop();
    startSavepointLoop();
    return Status::OK();
}

Status DB::close() {
    if (!is_open_) {
        return Status::OK();
    }

    Status s1;

    // Drain all pending flushes before closing.
    if (write_buffer_ && !write_buffer_->empty()) {
        write_buffer_->drainFlush();
    }
    write_buffer_.reset();

    stopGCLoop();
    stopSavepointLoop();

    is_open_ = false;

    Status s2;
    if (storage_) {
        s2 = storage_->close();
        storage_.reset();
    }

    Status s3;
    if (global_index_) {
        s3 = global_index_->close();
        global_index_.reset();
    }

    Status s4;
    if (versions_) {
        s4 = versions_->close();
        versions_.reset();
    }

    // Compact manifest for fast recovery, then close.
    Status s5;
    if (manifest_) {
        s5 = manifest_->compact();
        manifest_->close();
        manifest_.reset();
    }

    if (!s2.ok()) return s2;
    if (!s3.ok()) return s3;
    if (!s4.ok()) return s4;
    return s5;
}

bool DB::isOpen() const {
    return is_open_;
}


// --- Point Operations ---

Status DB::put(const std::string& key, const std::string& value,
               const WriteOptions& options) {
    if (!isOpen()) {
        return Status::InvalidArgument("Database not open");
    }

    uint64_t version = versions_->allocateVersion();
    write_buffer_->put(key, version, value, false);
    versions_->commitVersion(version);

    if (options.sync) {
        write_buffer_->drainFlush();
    }
    return Status::OK();
}

Status DB::get(const std::string& key, std::string& value,
               const ReadOptions& options) {
    uint64_t version;
    return get(key, value, version, options);
}

Status DB::get(const std::string& key, std::string& value,
               uint64_t& version, const ReadOptions& options) {
    // Pack the snapshot version as an upper bound: include tombstones at that version.
    uint64_t upper_bound;
    if (options.snapshot) {
        versions_->waitForCommitted(options.snapshot->version());
        upper_bound = (options.snapshot->version() << internal::PackedVersion::kVersionShift)
                      | internal::PackedVersion::kTombstoneMask;
    } else {
        upper_bound = UINT64_MAX;
    }
    return getByVersion(key, upper_bound, value, version, options);
}

Status DB::getByVersion(const std::string& key, uint64_t upper_bound,
                        std::string& value, const ReadOptions& options) {
    uint64_t entry_version;
    return getByVersion(key, upper_bound, value, entry_version, options);
}

Status DB::resolve(const std::string& key, uint64_t upper_bound,
                   ResolveResult& result) {
    if (!isOpen()) {
        return Status::InvalidArgument("Database not open");
    }

    // 1. Check WriteBuffer first (active + immutables).
    if (write_buffer_->getByVersion(key, upper_bound,
                                    result.wb_value, result.wb_version,
                                    result.wb_tombstone)) {
        result.wb_hit = true;
        return Status::OK();
    }

    // 2. Fall through to GlobalIndex -> Segment.
    uint64_t gi_packed_version;
    uint32_t gi_segment_id;
    uint64_t hkey = internal::dhtHashBytes(key.data(), key.size());
    if (!global_index_->get(hkey, upper_bound, gi_packed_version, gi_segment_id)) {
        return Status::NotFound(key);
    }

    result.wb_hit = false;
    result.gi_packed_version = gi_packed_version;
    result.segment = storage_->getSegment(gi_segment_id);
    assert(result.segment && "GlobalIndex references non-existent segment");
    return Status::OK();
}

Status DB::getByVersion(const std::string& key, uint64_t upper_bound,
                        std::string& value, uint64_t& entry_version,
                        const ReadOptions& /*options*/) {
    ResolveResult r;
    Status s = resolve(key, upper_bound, r);
    if (!s.ok()) return s;

    if (r.wb_hit) {
        if (r.wb_tombstone) return Status::NotFound(key);
        value = std::move(r.wb_value);
        entry_version = r.wb_version;
        return Status::OK();
    }

    // Check tombstone from the packed version in GI — no segment I/O needed.
    internal::PackedVersion gi_pv(r.gi_packed_version);
    if (gi_pv.tombstone()) return Status::NotFound(key);

    // Read the value from segment.
    internal::LogEntry entry;
    s = r.segment->get(key, upper_bound, entry);
    if (!s.ok()) return s;

    value = std::move(entry.value);
    entry_version = gi_pv.version();
    return Status::OK();
}

Status DB::remove(const std::string& key, const WriteOptions& options) {
    if (!isOpen()) {
        return Status::InvalidArgument("Database not open");
    }

    uint64_t version = versions_->allocateVersion();
    write_buffer_->put(key, version, "", true);
    versions_->commitVersion(version);

    if (options.sync) {
        write_buffer_->drainFlush();
    }
    return Status::OK();
}

Status DB::exists(const std::string& key, bool& exists,
                  const ReadOptions& options) {
    uint64_t upper_bound;
    if (options.snapshot) {
        versions_->waitForCommitted(options.snapshot->version());
        upper_bound = (options.snapshot->version() << internal::PackedVersion::kVersionShift)
                      | internal::PackedVersion::kTombstoneMask;
    } else {
        upper_bound = UINT64_MAX;
    }

    ResolveResult r;
    Status s = resolve(key, upper_bound, r);
    if (s.isNotFound()) {
        exists = false;
        return Status::OK();
    }
    if (!s.ok()) return s;

    if (r.wb_hit) {
        exists = !r.wb_tombstone;
        return Status::OK();
    }

    // GI stores packed version — check tombstone from LSB, no segment I/O.
    exists = !(r.gi_packed_version & internal::PackedVersion::kTombstoneMask);
    return Status::OK();
}

// --- Batch Operations ---

Status DB::write(const WriteBatch& batch, const WriteOptions& options) {
    if (!isOpen()) {
        return Status::InvalidArgument("Database not open");
    }

    if (batch.empty()) {
        return Status::OK();
    }

    // All operations in batch get the same version.
    // committed_version_ fence ensures snapshots see all-or-nothing.
    // Two-phase bucket locking inside putBatch() guarantees atomicity
    // at the Memtable level.
    uint64_t version = versions_->allocateVersion();

    std::vector<internal::Memtable::BatchOp> ops;
    ops.reserve(batch.operations().size());
    for (const auto& op : batch.operations()) {
        ops.push_back({&op.key, &op.value,
                       op.type == WriteBatch::OpType::kDelete});
    }
    write_buffer_->putBatch(ops, version);
    versions_->commitVersion(version);

    if (options.sync) {
        write_buffer_->drainFlush();
    }
    return Status::OK();
}

Status DB::read(ReadBatch& batch, const ReadOptions& options) {
    if (!isOpen()) {
        return Status::InvalidArgument("Database not open");
    }

    batch.clearResults();

    if (batch.empty()) {
        return Status::OK();
    }

    // Use caller's snapshot if provided, otherwise create one.
    Snapshot owned_snap(0);
    bool owns_snapshot = false;
    if (!options.snapshot) {
        owned_snap = createSnapshot();
        owns_snapshot = true;
    }
    const Snapshot& snap = owns_snapshot ? owned_snap : *options.snapshot;

    batch.setSnapshotVersion(snap.version());
    batch.reserveResults(batch.keys().size());

    ReadOptions snap_options = options;
    snap_options.snapshot = &snap;

    for (const auto& key : batch.keys()) {
        ReadResult result;
        result.key = key;
        result.status = get(key, result.value, snap_options);
        result.version = snap.version();
        batch.addResult(std::move(result));
    }

    if (owns_snapshot) {
        releaseSnapshot(owned_snap);
    }
    return Status::OK();
}

// --- Snapshots ---

Snapshot DB::createSnapshot() {
    return Snapshot(versions_->createSnapshot());
}

void DB::releaseSnapshot(const Snapshot& snapshot) {
    versions_->releaseSnapshot(snapshot.version());
}


// --- Maintenance ---

Status DB::flush() {
    if (!isOpen()) {
        return Status::InvalidArgument("Database not open");
    }

    write_buffer_->drainFlush();
    return Status::OK();
}

// --- Statistics ---

Status DB::getStats(DBStats& stats) const {
    if (!isOpen()) {
        return Status::InvalidArgument("Database not open");
    }

    stats.num_log_files = storage_->segmentCount();
    stats.total_log_size = storage_->totalDataSize();
    stats.global_index_size = global_index_->memoryUsage();
    stats.current_version = versions_->latestVersion();
    stats.oldest_version = versions_->oldestSnapshotVersion();

    stats.active_snapshots = versions_->activeSnapshotCount();

    // These would need GlobalIndex iteration to compute accurately
    stats.num_live_entries = global_index_->keyCount();
    stats.num_historical_entries = global_index_->entryCount() - global_index_->keyCount();

    return Status::OK();
}

Status DB::getPath(std::string& path) const {
    if (!isOpen()) {
        return Status::InvalidArgument("Database not open");
    }
    path = db_path_;
    return Status::OK();
}

uint64_t DB::getLatestVersion() const {
    return versions_->latestVersion();
}

uint64_t DB::getOldestVersion() const {
    return versions_->oldestSnapshotVersion();
}

// --- GC Daemon ---

void DB::startGCLoop() {
    if (options_.gc_interval_sec > 0 && options_.gc_policy != GCPolicy::MANUAL) {
        gc_stop_ = false;
        gc_thread_ = std::thread(&DB::gcLoop, this);
    }
}

void DB::stopGCLoop() {
    {
        std::lock_guard<std::mutex> lock(gc_mu_);
        gc_stop_ = true;
    }
    gc_cv_.notify_one();
    if (gc_thread_.joinable()) gc_thread_.join();
}

void DB::gcLoop() {
    std::unique_lock<std::mutex> lock(gc_mu_);
    while (!gc_stop_) {
        gc_cv_.wait_for(lock, std::chrono::seconds(options_.gc_interval_sec),
                        [this] { return gc_stop_; });
        if (gc_stop_) break;
        lock.unlock();

        double ratio = estimateDeadRatio();
        if (ratio > options_.gc_threshold) {
            runGC();
        }

        lock.lock();
    }
}

double DB::estimateDeadRatio() const {
    return global_index_->estimateDeadRatio();
}

std::vector<uint32_t> DB::selectGCInputs() {
    auto all_ids = storage_->getSegmentIds();
    std::vector<uint32_t> input_ids;
    if (all_ids.size() <= 1) return input_ids;

    uint32_t newest_id = all_ids.back();
    for (uint32_t id : all_ids) {
        if (id == newest_id) break;
        if (input_ids.size() >= static_cast<size_t>(options_.gc_max_segments))
            break;
        auto* seg = storage_->getSegment(id);
        if (seg && seg->state() == internal::Segment::State::kReadable) {
            input_ids.push_back(id);
        }
    }
    return input_ids;
}

Status DB::mergeSegments(const std::vector<uint32_t>& input_ids) {
    // Pin input segments.
    std::vector<const internal::Segment*> inputs;
    for (uint32_t id : input_ids) {
        storage_->pinSegment(id);
        inputs.push_back(storage_->getSegment(id));
    }

    auto snapshot_versions = versions_->snapshotVersions();

    // Merge under BatchGuard.
    internal::GC::Result result;
    Status s;
    {
        internal::GlobalIndex::BatchGuard guard(*global_index_);
        s = internal::GC::merge(
            snapshot_versions, inputs, options_.log_file_size,
            [this](uint32_t id) { return storage_->segmentPath(id); },
            [this]() { return storage_->allocateSegmentId(); },
            [this](uint64_t hkey, uint64_t pv, uint32_t old_id,
                   uint32_t new_id) {
                global_index_->relocate(hkey, pv, old_id, new_id);
            },
            [this](uint64_t hkey, uint64_t pv, uint32_t old_id) {
                global_index_->eliminate(hkey, pv, old_id);
            },
            result);
        if (!s.ok()) {
            for (uint32_t id : input_ids) storage_->unpinSegment(id);
            return s;
        }
        s = global_index_->commitGC();
    }
    if (!s.ok()) {
        for (uint32_t id : input_ids) storage_->unpinSegment(id);
        return s;
    }

    // Adopt output segments and register in Manifest.
    std::vector<uint32_t> output_ids;
    for (auto& seg : result.outputs) {
        uint32_t oid = seg.getId();
        output_ids.push_back(oid);
        storage_->adoptSegment(oid, std::move(seg));
    }
    return storage_->registerSegments(output_ids);
}

Status DB::runGC() {
    auto input_ids = selectGCInputs();
    if (input_ids.empty()) return Status::OK();

    Status s = mergeSegments(input_ids);
    if (!s.ok()) return s;

    for (uint32_t id : input_ids) {
        storage_->unpinSegment(id);
        storage_->removeSegment(id);
    }
    return Status::OK();
}

// --- Savepoint Daemon ---

void DB::startSavepointLoop() {
    if (options_.savepoint_interval_sec > 0) {
        sp_stop_ = false;
        sp_thread_ = std::thread(&DB::savepointLoop, this);
    }
}

void DB::stopSavepointLoop() {
    {
        std::lock_guard<std::mutex> lock(sp_mu_);
        sp_stop_ = true;
    }
    sp_cv_.notify_one();
    if (sp_thread_.joinable()) sp_thread_.join();
}

void DB::savepointLoop() {
    std::unique_lock<std::mutex> lock(sp_mu_);
    while (!sp_stop_) {
        sp_cv_.wait_for(lock, std::chrono::seconds(options_.savepoint_interval_sec),
                        [this] { return sp_stop_; });
        if (sp_stop_) break;
        lock.unlock();

        global_index_->maybeSavepoint();

        lock.lock();
    }
}

}  // namespace kvlite
