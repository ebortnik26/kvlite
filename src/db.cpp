#include "kvlite/db.h"

#include <algorithm>
#include <memory>
#include <vector>

#include "internal/manifest.h"
#include "internal/version_manager.h"
#include "internal/global_index.h"
#include "internal/segment_storage_manager.h"
#include "internal/write_buffer.h"
#include "internal/log_entry.h"
#include "internal/segment.h"
#include "internal/segment_index.h"
#include "internal/entry_stream.h"

namespace kvlite {

// --- DB::Snapshot Implementation ---

DB::Snapshot::Snapshot(DB* db, uint64_t version)
    : db_(db), version_(version) {}

DB::Snapshot::~Snapshot() {
    if (db_) {
        db_->versions_->releaseSnapshot(version_);
    }
}

DB::Snapshot::Snapshot(Snapshot&& other) noexcept
    : db_(other.db_), version_(other.version_) {
    other.db_ = nullptr;
}

DB::Snapshot& DB::Snapshot::operator=(Snapshot&& other) noexcept {
    if (this != &other) {
        if (db_) {
            db_->versions_->releaseSnapshot(version_);
        }
        db_ = other.db_;
        version_ = other.version_;
        other.db_ = nullptr;
    }
    return *this;
}

Status DB::Snapshot::get(const std::string& key, std::string& value,
                         const ReadOptions& options) const {
    uint64_t entry_version;
    return get(key, value, entry_version, options);
}

Status DB::Snapshot::get(const std::string& key, std::string& value,
                         uint64_t& entry_version, const ReadOptions& options) const {
    if (!db_) {
        return Status::InvalidArgument("Snapshot is invalid");
    }
    return db_->getByVersion(key, version_, value, entry_version, options);
}

Status DB::Snapshot::exists(const std::string& key, bool& exists,
                            const ReadOptions& options) const {
    std::string value;
    Status s = get(key, value, options);
    if (s.ok()) {
        exists = true;
        return Status::OK();
    } else if (s.isNotFound()) {
        exists = false;
        return Status::OK();
    }
    return s;
}

uint64_t DB::Snapshot::version() const { return version_; }
bool DB::Snapshot::isValid() const { return db_ != nullptr; }
void DB::Snapshot::detach() { db_ = nullptr; }

// --- DB::Iterator Implementation ---
//
// Scans segment files from newest to oldest. For each entry, consults GlobalIndex
// to check if it's the latest version at the snapshot. Only returns entries
// that are the current (non-tombstone) value for their key.

class DB::Iterator::Impl {
public:
    // Owned snapshot: iterator manages its own snapshot lifetime
    Impl(DB* db, std::unique_ptr<Snapshot> owned_snapshot)
        : db_(db),
          owned_snapshot_(std::move(owned_snapshot)),
          snapshot_(owned_snapshot_.get()) {
        pinned_wb_ = db_->write_buffer_.get();
        pinned_wb_->pin();
        init();
    }

    // Borrowed snapshot: caller manages snapshot lifetime
    Impl(DB* db, const Snapshot* borrowed_snapshot)
        : db_(db),
          snapshot_(borrowed_snapshot) {
        pinned_wb_ = db_->write_buffer_.get();
        pinned_wb_->pin();
        init();
    }

    ~Impl() {
        merged_.reset();
        for (uint32_t id : pinned_segment_ids_) {
            db_->storage_->unpinSegment(id);
        }
        pinned_wb_->unpin();
        db_->cleanupRetiredBuffers();
    }

    Status next(std::string& key, std::string& value, uint64_t& version) {
        if (!valid_) {
            return Status::NotFound("Iterator exhausted");
        }

        key = std::move(current_key_);
        value = std::move(current_value_);
        version = current_version_;

        findNextValid();
        return Status::OK();
    }

    const Snapshot& snapshot() const { return *snapshot_; }

private:
    void init() {
        auto segment_ids = db_->storage_->getSegmentIds();
        uint64_t snap_ver = snapshot_->version();

        std::vector<std::unique_ptr<internal::EntryStream>> streams;

        // Write buffer stream (captures in-memory entries at snapshot).
        auto wb_stream = internal::stream::scanWriteBuffer(
            *pinned_wb_, snap_ver);
        if (wb_stream->valid()) {
            streams.push_back(std::move(wb_stream));
        }

        // Per-segment streams. Pre-computes visibility at construction.
        for (uint32_t id : segment_ids) {
            auto* seg = db_->storage_->getSegment(id);
            if (!seg) continue;
            db_->storage_->pinSegment(id);
            pinned_segment_ids_.push_back(id);
            streams.push_back(internal::stream::scanLatestConsistent(
                *db_->global_index_, id, snap_ver,
                seg->logFile(), seg->dataSize()));
        }

        merged_ = internal::stream::merge(std::move(streams));
        findNextValid();
    }

    void findNextValid() {
        // MergeStream yields entries in (hash asc, version asc) order.
        // For the same hash, advance to the last entry (highest version = latest).
        // If the latest is a tombstone, skip the key.
        //
        // Zero-copy strategy: skip intermediate entries without reading
        // key/value. Only assign from the final entry per hash.
        while (merged_->valid()) {
            uint64_t hash = merged_->entry().hash;

            // Peek ahead: if the next entry has the same hash, skip this one.
            // We want to find the last entry for this hash (latest version).
            while (true) {
                // Capture the current (possibly last) entry via string_view.
                const auto& e = merged_->entry();
                current_key_.assign(e.key.data(), e.key.size());
                current_value_.assign(e.value.data(), e.value.size());
                current_version_ = e.version;
                bool tombstone = e.tombstone;

                merged_->next();

                if (!merged_->valid() || merged_->entry().hash != hash) {
                    // This was the last entry for this hash.
                    if (!tombstone) {
                        valid_ = true;
                        return;
                    }
                    break;  // Tombstone — skip this key, continue outer loop.
                }
                // More entries with the same hash — loop to overwrite with the next one.
            }
        }
        valid_ = false;
    }

    DB* db_;
    std::unique_ptr<Snapshot> owned_snapshot_;  // non-null when iterator owns its snapshot
    const Snapshot* snapshot_;                  // always valid — points to owned or borrowed

    internal::WriteBuffer* pinned_wb_ = nullptr;
    std::vector<uint32_t> pinned_segment_ids_;

    std::unique_ptr<internal::EntryStream> merged_;

    bool valid_ = false;
    std::string current_key_;
    std::string current_value_;
    uint64_t current_version_ = 0;
};

DB::Iterator::Iterator(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
DB::Iterator::~Iterator() = default;

DB::Iterator::Iterator(Iterator&& other) noexcept = default;
DB::Iterator& DB::Iterator::operator=(Iterator&& other) noexcept = default;

Status DB::Iterator::next(std::string& key, std::string& value) {
    uint64_t version;
    return next(key, value, version);
}

Status DB::Iterator::next(std::string& key, std::string& value, uint64_t& version) {
    return impl_->next(key, value, version);
}

const DB::Snapshot& DB::Iterator::snapshot() const {
    return impl_->snapshot();
}

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
    global_index_ = std::make_unique<internal::GlobalIndex>();
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

    s = global_index_->recover();
    if (!s.ok()) {
        versions_->close();
        global_index_->close();
        versions_.reset();
        global_index_.reset();
        manifest_->close();
        manifest_.reset();
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

    // Rebuild GlobalIndex from recovered segments.
    {
        auto segment_ids = storage_->getSegmentIds();
        for (uint32_t id : segment_ids) {
            auto* seg = storage_->getSegment(id);
            if (!seg) continue;
            seg->index().forEach([&](uint32_t offset, uint32_t version) {
                internal::LogEntry entry;
                Status rs = seg->readEntry(offset, entry);
                if (rs.ok()) {
                    global_index_->put(entry.key, entry.version(), id);
                }
            });
        }
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

Status DB::get(const std::string& key, std::string& value, uint64_t& version,
               const ReadOptions& options) {
    if (!isOpen()) {
        return Status::InvalidArgument("Database not open");
    }

    // 1. Check WriteBuffer first (covers unflushed entries).
    {
        std::string wval;
        uint64_t wver;
        bool tombstone;
        if (write_buffer_->get(key, wval, wver, tombstone)) {
            if (tombstone) return Status::NotFound(key);
            value = std::move(wval);
            version = wver;
            return Status::OK();
        }
    }

    // 2. Fall through to GlobalIndex -> Segment (flushed entries).
    std::vector<uint32_t> segment_ids;
    std::vector<uint64_t> versions;
    if (!global_index_->get(key, segment_ids, versions)) {
        return Status::NotFound(key);
    }

    for (uint32_t sid : segment_ids) {
        auto* seg = storage_->getSegment(sid);
        if (!seg) continue;

        internal::LogEntry entry;
        Status s = seg->getLatest(key, entry);
        if (s.ok()) {
            if (entry.tombstone()) return Status::NotFound(key);
            value = std::move(entry.value);
            version = entry.version();
            return Status::OK();
        }
        if (!s.isNotFound()) {
            return s;  // propagate non-NotFound errors
        }
    }
    return Status::NotFound(key);
}

Status DB::getByVersion(const std::string& key, uint64_t upper_bound,
                        std::string& value, const ReadOptions& options) {
    uint64_t entry_version;
    return getByVersion(key, upper_bound, value, entry_version, options);
}

Status DB::getByVersion(const std::string& key, uint64_t upper_bound,
                        std::string& value, uint64_t& entry_version,
                        const ReadOptions& options) {
    if (!isOpen()) {
        return Status::InvalidArgument("Database not open");
    }

    // 1. Check WriteBuffer first.
    {
        std::string wval;
        uint64_t wver;
        bool tombstone;
        if (write_buffer_->getByVersion(key, upper_bound, wval, wver, tombstone)) {
            if (tombstone) return Status::NotFound(key);
            value = std::move(wval);
            entry_version = wver;
            return Status::OK();
        }
    }

    // 2. Fall through to GlobalIndex -> Segment.
    std::vector<uint32_t> segment_ids;
    std::vector<uint64_t> versions;
    if (!global_index_->get(key, segment_ids, versions)) {
        return Status::NotFound(key);
    }

    for (uint32_t sid : segment_ids) {
        auto* seg = storage_->getSegment(sid);
        if (!seg) continue;

        internal::LogEntry entry;
        Status s = seg->get(key, upper_bound, entry);
        if (s.ok()) {
            if (entry.tombstone()) return Status::NotFound(key);
            value = std::move(entry.value);
            entry_version = entry.version();
            return Status::OK();
        }
        if (!s.isNotFound()) {
            return s;
        }
    }
    return Status::NotFound(key);
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

    if (options.sync ||
        write_buffer_->memoryUsage() >= options_.write_buffer_size) {
        return flush();
    }
    return Status::OK();
}

Status DB::exists(const std::string& key, bool& exists,
                  const ReadOptions& options) {
    std::string value;
    Status s = get(key, value, options);
    if (s.ok()) {
        exists = true;
        return Status::OK();
    } else if (s.isNotFound()) {
        exists = false;
        return Status::OK();
    }
    return s;
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
    // Hold batch_mutex_ so that a concurrent ReadBatch snapshot
    // either sees all keys or none of them.
    // Two-phase bucket locking inside putBatch() guarantees atomicity
    // at the WriteBuffer level.
    {
        std::lock_guard<std::mutex> lock(batch_mutex_);
        uint64_t version = versions_->allocateVersion();

        std::vector<internal::WriteBuffer::BatchOp> ops;
        ops.reserve(batch.operations().size());
        for (const auto& op : batch.operations()) {
            ops.push_back({&op.key, &op.value,
                           op.type == WriteBatch::OpType::kDelete});
        }
        write_buffer_->putBatch(ops, version);
    }

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

    // Create a snapshot for consistent reads.
    std::unique_ptr<Snapshot> snap;
    {
        std::lock_guard<std::mutex> lock(batch_mutex_);
        Status s = createSnapshot(snap);
        if (!s.ok()) return s;
    }

    uint64_t snapshot_version = snap->version();
    batch.setSnapshotVersion(snapshot_version);
    batch.reserveResults(batch.keys().size());

    for (const auto& key : batch.keys()) {
        ReadResult result;
        result.key = key;

        uint64_t entry_version;
        result.status = snap->get(key, result.value, entry_version, options);
        result.version = snapshot_version;
        batch.addResult(std::move(result));
    }

    releaseSnapshot(std::move(snap));
    return Status::OK();
}

// --- Snapshots ---

Status DB::createSnapshot(std::unique_ptr<Snapshot>& snapshot) {
    if (!isOpen()) {
        return Status::InvalidArgument("Database not open");
    }

    uint64_t version = versions_->createSnapshot();
    snapshot = std::unique_ptr<Snapshot>(new Snapshot(this, version));
    return Status::OK();
}

Status DB::releaseSnapshot(std::unique_ptr<Snapshot> snapshot) {
    if (snapshot) {
        snapshot->detach();  // Prevent double-release in destructor
        versions_->releaseSnapshot(snapshot->version());
    }
    return Status::OK();
}

// --- Iteration ---

Status DB::createIterator(std::unique_ptr<Iterator>& iterator) {
    if (!isOpen()) {
        return Status::InvalidArgument("Database not open");
    }

    std::unique_ptr<Snapshot> snap;
    Status s = createSnapshot(snap);
    if (!s.ok()) {
        return s;
    }

    auto impl = std::make_unique<Iterator::Impl>(this, std::move(snap));
    iterator = std::unique_ptr<Iterator>(new Iterator(std::move(impl)));
    return Status::OK();
}

Status DB::createIterator(const Snapshot& snapshot, std::unique_ptr<Iterator>& iterator) {
    if (!isOpen()) {
        return Status::InvalidArgument("Database not open");
    }

    if (!snapshot.isValid()) {
        return Status::InvalidArgument("Snapshot is invalid");
    }

    auto impl = std::make_unique<Iterator::Impl>(this, &snapshot);
    iterator = std::unique_ptr<Iterator>(new Iterator(std::move(impl)));
    return Status::OK();
}

// --- Maintenance ---

Status DB::flush() {
    if (!isOpen()) {
        return Status::InvalidArgument("Database not open");
    }

    if (write_buffer_->empty()) {
        return Status::OK();
    }

    Status s = storage_->createSegment(current_segment_id_);
    if (!s.ok()) return s;

    auto* seg = storage_->getSegment(current_segment_id_);
    s = write_buffer_->flush(*seg, current_segment_id_, *global_index_);
    if (!s.ok()) {
        return s;
    }

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
