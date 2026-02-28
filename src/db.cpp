#include "kvlite/db.h"

#include <algorithm>
#include <cassert>
#include <memory>
#include <vector>

#include "internal/delta_hash_table.h"
#include "internal/manifest.h"
#include "internal/version_manager.h"
#include "internal/global_index.h"
#include "internal/segment_storage_manager.h"
#include "internal/write_buffer.h"
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
    global_index_opts.snapshot_interval = options.global_index_snapshot_interval;
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

    internal::SegmentStorageManager::Options sm_opts;
    sm_opts.purge_untracked_files = options.purge_untracked_files;
    s = storage_->open(path, sm_opts);
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

    // Initialize write buffer and allocate first segment ID
    write_buffer_ = std::make_unique<internal::WriteBuffer>();
    current_segment_id_ = storage_->allocateSegmentId();

    is_open_ = true;
    return Status::OK();
}

Status DB::close() {
    if (!is_open_) {
        return Status::OK();
    }

    Status s1;

    // Flush write buffer before closing (must happen while is_open_ is true)
    if (write_buffer_ && !write_buffer_->empty()) {
        s1 = flush();
    }
    write_buffer_.reset();

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
        manifest_->set("clean_close", "1");
        s5 = manifest_->compact();
        manifest_->close();
        manifest_.reset();
    }

    if (!s1.ok()) return s1;
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

    if (clean_close_persisted_) {
        manifest_->set("clean_close", "0");
        clean_close_persisted_ = false;
    }

    uint64_t version = versions_->allocateVersion();
    write_buffer_->put(key, version, value, false);
    versions_->commitVersion(version);

    if (options.sync ||
        write_buffer_->memoryUsage() >= options_.write_buffer_size) {
        return flush();
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

    // 1. Check WriteBuffer first.
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

    if (clean_close_persisted_) {
        manifest_->set("clean_close", "0");
        clean_close_persisted_ = false;
    }

    uint64_t version = versions_->allocateVersion();
    write_buffer_->put(key, version, "", true);
    versions_->commitVersion(version);

    if (options.sync ||
        write_buffer_->memoryUsage() >= options_.write_buffer_size) {
        return flush();
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

    if (clean_close_persisted_) {
        manifest_->set("clean_close", "0");
        clean_close_persisted_ = false;
    }

    // All operations in batch get the same version.
    // committed_version_ fence ensures snapshots see all-or-nothing.
    // Two-phase bucket locking inside putBatch() guarantees atomicity
    // at the WriteBuffer level.
    uint64_t version = versions_->allocateVersion();

    std::vector<internal::WriteBuffer::BatchOp> ops;
    ops.reserve(batch.operations().size());
    for (const auto& op : batch.operations()) {
        ops.push_back({&op.key, &op.value,
                       op.type == WriteBatch::OpType::kDelete});
    }
    write_buffer_->putBatch(ops, version);
    versions_->commitVersion(version);

    if (options.sync ||
        write_buffer_->memoryUsage() >= options_.write_buffer_size) {
        return flush();
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

    if (write_buffer_->empty()) {
        return Status::OK();
    }

    Status s = storage_->createSegment(current_segment_id_, false);
    if (!s.ok()) return s;

    auto* seg = storage_->getSegment(current_segment_id_);
    auto result = write_buffer_->flush(*seg);
    if (!result.status.ok()) return result.status;

    s = storage_->registerSegments({current_segment_id_});
    if (!s.ok()) return s;

    for (const auto& e : result.entries) {
        s = global_index_->put(e.hkey, e.packed_ver, current_segment_id_);
        if (!s.ok()) return s;
    }

    s = global_index_->commitWB(result.max_version);
    if (!s.ok()) return s;

    current_segment_id_ = storage_->allocateSegmentId();

    // Clear or retire the write buffer.
    if (write_buffer_->pinCount() == 0) {
        write_buffer_->clear();
    } else {
        // Iterators are pinning this buffer — retire it and allocate a fresh one.
        retired_buffers_.push_back(std::move(write_buffer_));
        write_buffer_ = std::make_unique<internal::WriteBuffer>();
    }

    return Status::OK();
}

void DB::cleanupRetiredBuffers() {
    retired_buffers_.erase(
        std::remove_if(retired_buffers_.begin(), retired_buffers_.end(),
                        [](const std::unique_ptr<internal::WriteBuffer>& wb) {
                            return wb->pinCount() == 0;
                        }),
        retired_buffers_.end());
}

// --- Statistics ---

Status DB::getStats(DBStats& stats) const {
    if (!isOpen()) {
        return Status::InvalidArgument("Database not open");
    }

    stats.num_log_files = storage_->segmentCount();
    stats.total_log_size = storage_->totalDataSize();
    stats.global_index_size = global_index_->memoryUsage();
    stats.segment_index_cache_size = 0;   // no separate cache
    stats.segment_index_cached_count = 0; // no separate cache
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

}  // namespace kvlite
