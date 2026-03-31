#include "kvlite/db.h"

#include <algorithm>
#include <cassert>
#include <memory>
#include <vector>

#include "internal/delta_hash_table.h"
#include "internal/gc.h"
#include "internal/periodic_daemon.h"
#include "internal/manifest.h"
#include "internal/version_manager.h"
#include "internal/global_index.h"
#include "internal/segment_storage_manager.h"
#include "internal/write_buffer.h"
#include "internal/memtable.h"
#include "internal/log_entry.h"
#include "internal/segment.h"
#include "internal/profiling.h"

namespace kvlite {

using internal::trackTime;

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

    // Each subsystem is initialized in dependency order. On any failure,
    // teardown() closes everything opened so far.

    manifest_ = std::make_unique<internal::Manifest>();
    Status s = manifest_->open(path);
    if (!s.ok() && s.isNotFound() && options.create_if_missing) {
        s = manifest_->create(path);
    }
    if (!s.ok()) { teardown(); return s; }

    versions_ = std::make_unique<internal::VersionManager>(*manifest_);
    s = versions_->open(internal::VersionManager::Options());
    if (!s.ok()) { teardown(); return s; }

    s = versions_->recover();
    if (!s.ok()) { teardown(); return s; }

    // Storage must open before GlobalIndex — recovery reads segment lineages.
    storage_ = std::make_unique<internal::SegmentStorageManager>(
        *manifest_, options.buffered_writes, options.segment_partitions);
    s = storage_->open(path);
    if (!s.ok()) { teardown(); return s; }

    global_index_ = std::make_unique<internal::GlobalIndex>(*manifest_);
    internal::GlobalIndex::Options gi_opts;
    gi_opts.savepoint_threads = 4;
    s = global_index_->open(path, gi_opts, *storage_);
    if (!s.ok()) { teardown(); return s; }

    initWriteBuffer(options);
    is_open_ = true;
    startDaemons(options);

    return Status::OK();
}

void DB::initWriteBuffer(const Options& options) {
    internal::WriteBuffer::Options wb_opts;
    wb_opts.memtable_size = options.memtable_size;
    wb_opts.flush_depth = options.flush_depth;

    auto flush_fn = [this](internal::Memtable& mt) -> Status {
        auto t0 = internal::now();

        uint32_t seg_id = storage_->allocateSegmentId();
        Status s = storage_->createSegment(seg_id);
        if (!s.ok()) return s;

        auto* seg = storage_->getSegment(seg_id);
        seg->setLineageType(internal::LineageType::kFlush);
        seg->reserveLineage(mt.entryCount());

        auto result = mt.flush(*seg, versions_->snapshotVersions(),
                              storage_->flushPool());
        if (!result.status.ok()) return result.status;

        // Segment is now sealed (data + index + lineage + footer on disk).
        // Batch-apply to in-memory GlobalIndex (one lock per bucket run).
        global_index_->applyPutBatch(
            result.entries.data(), result.entries.size(), seg_id);

        // Accumulate SI encode stats (write path only).
        si_encode_count_.fetch_add(seg->siEncodeCount(), std::memory_order_relaxed);
        si_encode_total_ns_.fetch_add(seg->siEncodeTotalNs(), std::memory_order_relaxed);

        trackTime<std::chrono::microseconds>(flush_count_, flush_total_us_, t0);
        return s;
    };

    write_buffer_ = std::make_unique<internal::WriteBuffer>(wb_opts, flush_fn);
}

void DB::startDaemons(const Options& options) {
    if (options.gc_interval_sec > 0 && options.gc_policy != GCPolicy::MANUAL) {
        gc_daemon_ = std::make_unique<internal::PeriodicDaemon>();
        gc_daemon_->start(options.gc_interval_sec, [this] {
            if (global_index_->estimateDeadRatio() > options_.gc_threshold) {
                runGC();
            }
        });
    }

    if (options.savepoint_interval_sec > 0) {
        sp_daemon_ = std::make_unique<internal::PeriodicDaemon>();
        sp_daemon_->start(options.savepoint_interval_sec, [this] {
            auto t0 = internal::now();
            auto seg_ids = storage_->getSegmentIds();
            uint32_t max_seg_id = seg_ids.empty() ? 0 : seg_ids.back();
            global_index_->maybeSavepoint(max_seg_id);
            trackTime<std::chrono::microseconds>(savepoint_count_, savepoint_total_us_, t0);
        });
    }

    if (options.version_prune_interval_sec > 0) {
        prune_daemon_ = std::make_unique<internal::PeriodicDaemon>();
        prune_daemon_->start(options.version_prune_interval_sec, [this] {
            global_index_->pruneStaleVersions(versions_->snapshotVersions());
        });
    }
}

void DB::teardown() {
    write_buffer_.reset();
    gc_daemon_.reset();
    sp_daemon_.reset();
    prune_daemon_.reset();
    if (global_index_) { global_index_->close(); global_index_.reset(); }
    if (storage_)      { storage_->close(); storage_.reset(); }
    if (versions_)     { versions_->close(); versions_.reset(); }
    if (manifest_)     { manifest_->close(); manifest_.reset(); }
    is_open_ = false;
}

Status DB::close() {
    if (!is_open_) {
        return Status::OK();
    }

    // Drain all pending flushes before closing.
    if (write_buffer_ && !write_buffer_->empty()) {
        write_buffer_->drainFlush();
    }

    // Store final savepoint with current max segment ID.
    if (global_index_ && storage_) {
        auto seg_ids = storage_->getSegmentIds();
        uint32_t max_seg_id = seg_ids.empty() ? 0 : seg_ids.back();
        global_index_->storeSavepoint(max_seg_id);
    }

    // Compact manifest for fast recovery before teardown closes it.
    Status s;
    if (manifest_) {
        s = manifest_->compact();
    }

    teardown();
    return s;
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
    if (key.empty()) {
        return Status::InvalidArgument("Empty key");
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
    if (key.empty()) {
        return Status::InvalidArgument("Empty key");
    }

    uint64_t hkey = internal::dhtHashBytes(key.data(), key.size());

    if (write_buffer_->getByVersion(hkey, upper_bound,
                                    result.wb_value, result.wb_version,
                                    result.wb_tombstone)) {
        result.wb_hit = true;
        return Status::OK();
    }

    uint64_t gi_packed_version;
    uint32_t gi_segment_id;
    if (!global_index_->get(hkey, upper_bound, gi_packed_version, gi_segment_id)) {
        return Status::NotFound(key);
    }

    result.wb_hit = false;
    result.gi_packed_version = gi_packed_version;
    result.hkey = hkey;
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

    internal::PackedVersion gi_pv(r.gi_packed_version);
    if (gi_pv.tombstone()) return Status::NotFound(key);

    s = r.segment->getValue(r.hkey, upper_bound, value);
    if (!s.ok()) return s;

    entry_version = gi_pv.version();
    return Status::OK();
}

Status DB::remove(const std::string& key, const WriteOptions& options) {
    if (!isOpen()) {
        return Status::InvalidArgument("Database not open");
    }
    if (key.empty()) {
        return Status::InvalidArgument("Empty key");
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

    stats.num_live_entries = global_index_->keyCount();
    stats.num_historical_entries = global_index_->entryCount() - global_index_->keyCount();

    stats.flush_count = flush_count_.load(std::memory_order_relaxed);
    stats.flush_total_us = flush_total_us_.load(std::memory_order_relaxed);
    stats.gc_count = gc_count_.load(std::memory_order_relaxed);
    stats.gc_total_us = gc_total_us_.load(std::memory_order_relaxed);
    stats.savepoint_count = savepoint_count_.load(std::memory_order_relaxed);
    stats.savepoint_total_us = savepoint_total_us_.load(std::memory_order_relaxed);

    stats.stall_count = write_buffer_->stallCount();
    stats.stall_total_us = write_buffer_->stallTotalUs();

    stats.dht_encode_count = global_index_->dhtEncodeCount();
    stats.dht_encode_total_ns = global_index_->dhtEncodeTotalNs();
    stats.dht_decode_count = global_index_->dhtDecodeCount();
    stats.dht_decode_total_ns = global_index_->dhtDecodeTotalNs();
    stats.dht_ext_count = global_index_->dhtExtCount();
    stats.dht_num_buckets = global_index_->dhtNumBuckets();

    stats.si_encode_count = si_encode_count_.load(std::memory_order_relaxed);
    stats.si_encode_total_ns = si_encode_total_ns_.load(std::memory_order_relaxed);

    // SI decode stats: sum across all live segments (read-path decodes).
    uint64_t si_dec_count = 0, si_dec_ns = 0;
    for (uint32_t id : storage_->getSegmentIds()) {
        const auto* seg = storage_->getSegment(id);
        if (!seg) continue;
        si_dec_count += seg->siDecodeCount();
        si_dec_ns += seg->siDecodeTotalNs();
    }
    stats.si_decode_count = si_dec_count;
    stats.si_decode_total_ns = si_dec_ns;

    stats.mt_ext_count = write_buffer_->extensionCount();
    stats.mt_num_buckets = internal::Memtable::numBuckets();

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

    // Merge under BatchGuard (prevents savepoint during GC).
    // GC::merge creates output segments directly, records lineage on them,
    // and calls on_relocate/on_eliminate to update the in-memory GlobalIndex.
    internal::GC::Result result;
    Status s;
    {
        internal::GlobalIndex::BatchGuard guard(*global_index_);
        s = internal::GC::merge(
            snapshot_versions, inputs, options_.log_file_size,
            [this](uint32_t id) { return storage_->segmentBasePath(id); },
            [this]() {
                uint32_t id = storage_->allocateSegmentId();
                // Register in manifest immediately for crash recovery.
                storage_->registerSegmentId(id);
                return id;
            },
            [this](uint64_t hkey, uint64_t pv, uint32_t old_id,
                   uint32_t new_id) {
                global_index_->applyRelocate(hkey, pv, old_id, new_id);
            },
            [this](uint64_t hkey, uint64_t pv, uint32_t old_id) {
                global_index_->applyEliminate(hkey, pv, old_id);
            },
            result, options_.buffered_writes, options_.segment_partitions,
            storage_->flushPool());
        if (!s.ok()) {
            for (uint32_t id : input_ids) storage_->unpinSegment(id);
            return s;
        }
    }

    // Adopt output segments into storage's in-memory registry.
    for (auto& seg : result.outputs) {
        uint32_t oid = seg.getId();
        storage_->adoptSegment(oid, std::move(seg));
    }

    return Status::OK();
}

Status DB::runGC() {
    auto input_ids = selectGCInputs();
    if (input_ids.empty()) return Status::OK();

    auto t0 = internal::now();

    Status s = mergeSegments(input_ids);
    if (!s.ok()) return s;

    for (uint32_t id : input_ids) {
        storage_->unpinSegment(id);
        storage_->removeSegment(id);
    }

    trackTime<std::chrono::microseconds>(gc_count_, gc_total_us_, t0);

    return Status::OK();
}

}  // namespace kvlite
