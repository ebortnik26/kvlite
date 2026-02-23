#include "kvlite/db.h"

#include <algorithm>

#include "internal/manifest.h"
#include "internal/version_manager.h"
#include "internal/global_index_manager.h"
#include "internal/segment_storage_manager.h"
#include "internal/write_buffer.h"
#include "internal/log_entry.h"
#include "internal/segment.h"
#include "internal/segment_index.h"

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
        init();
    }

    // Borrowed snapshot: caller manages snapshot lifetime
    Impl(DB* db, const Snapshot* borrowed_snapshot)
        : db_(db),
          snapshot_(borrowed_snapshot) {
        init();
    }

    ~Impl() = default;

    Status next(std::string& key, std::string& value, uint64_t& version) {
        if (!valid_) {
            return Status::NotFound("Iterator exhausted");
        }

        key = current_key_;
        value = current_value_;
        version = current_version_;

        advanceAndFindNext();
        return Status::OK();
    }

    const Snapshot& snapshot() const { return *snapshot_; }

private:
    void init() {
        // Get segment IDs sorted ascending (oldest to newest)
        file_ids_ = db_->storage_->getSegmentIds();
        // Reverse to iterate newest to oldest
        std::reverse(file_ids_.begin(), file_ids_.end());

        if (!file_ids_.empty()) {
            current_file_idx_ = 0;
            loadCurrentFile();
            findNextValid();
        }
    }

    void loadCurrentFile() {
        if (current_file_idx_ >= file_ids_.size()) {
            current_seg_index_ = nullptr;
            seg_index_entries_.clear();
            seg_index_entry_idx_ = 0;
            return;
        }

        current_file_id_ = file_ids_[current_file_idx_];
        auto* seg = db_->storage_->getSegment(current_file_id_);
        current_seg_index_ = seg ? &seg->index() : nullptr;

        uint64_t snap_ver = snapshot_->version();

        // Collect all entries from this SegmentIndex
        seg_index_entries_.clear();
        if (current_seg_index_) {
            current_seg_index_->forEach([this, snap_ver](uint32_t offset, uint32_t version) {
                // Only consider entries within our snapshot
                if (version <= snap_ver) {
                    SegIndexEntry e;
                    e.version = version;
                    e.offset = offset;
                    seg_index_entries_.push_back(std::move(e));
                }
            });
        }
        seg_index_entry_idx_ = 0;
    }

    void advanceAndFindNext() {
        seg_index_entry_idx_++;
        findNextValid();
    }

    void findNextValid() {
        while (true) {
            // Move to next file if needed
            while (seg_index_entry_idx_ >= seg_index_entries_.size()) {
                current_file_idx_++;
                if (current_file_idx_ >= file_ids_.size()) {
                    valid_ = false;
                    return;
                }
                loadCurrentFile();
            }

            const auto& entry = seg_index_entries_[seg_index_entry_idx_];

            // Read the log entry to get key and value
            auto* seg = db_->storage_->getSegment(current_file_id_);
            if (!seg) {
                seg_index_entry_idx_++;
                continue;
            }

            internal::LogEntry log_entry;
            Status s = seg->readEntry(entry.offset, log_entry);
            if (!s.ok()) {
                seg_index_entry_idx_++;
                continue;
            }

            // Skip if we've already returned this key
            if (seen_keys_.count(log_entry.key)) {
                seg_index_entry_idx_++;
                continue;
            }

            // Check GlobalIndex: is this file the latest for this key?
            uint64_t latest_ver;
            uint32_t latest_fid;
            if (!db_->global_index_->getLatest(log_entry.key, latest_ver, latest_fid).ok()) {
                seg_index_entry_idx_++;
                continue;
            }

            if (latest_fid != current_file_id_) {
                // This file is not the latest for this key, skip
                seg_index_entry_idx_++;
                continue;
            }

            // Skip tombstones
            if (log_entry.tombstone()) {
                seen_keys_.insert(log_entry.key);
                seg_index_entry_idx_++;
                continue;
            }

            // Found a valid entry
            seen_keys_.insert(log_entry.key);
            current_key_ = std::move(log_entry.key);
            current_value_ = std::move(log_entry.value);
            current_version_ = log_entry.version();
            valid_ = true;
            return;
        }
    }

    struct SegIndexEntry {
        uint32_t version;
        uint32_t offset;
    };

    DB* db_;
    std::unique_ptr<Snapshot> owned_snapshot_;  // non-null when iterator owns its snapshot
    const Snapshot* snapshot_;                  // always valid — points to owned or borrowed

    std::vector<uint32_t> file_ids_;
    size_t current_file_idx_ = 0;
    uint32_t current_file_id_ = 0;
    const internal::SegmentIndex* current_seg_index_ = nullptr;

    std::vector<SegIndexEntry> seg_index_entries_;
    size_t seg_index_entry_idx_ = 0;

    std::set<std::string> seen_keys_;

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

DB::DB(DB&& other) noexcept = default;
DB& DB::operator=(DB&& other) noexcept = default;

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

    // Initialize GlobalIndex manager
    global_index_ = std::make_unique<internal::GlobalIndexManager>();
    internal::GlobalIndexManager::Options global_index_opts;
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

    // All operations in batch get the same version
    uint64_t version = versions_->allocateVersion();

    for (const auto& op : batch.operations()) {
        bool tombstone = (op.type == WriteBatch::OpType::kDelete);
        write_buffer_->put(op.key, version, op.value, tombstone);
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

    // Create implicit snapshot for consistent reads
    uint64_t snapshot_version = versions_->latestVersion();
    batch.setSnapshotVersion(snapshot_version);
    batch.reserveResults(batch.count());

    for (const auto& key : batch.keys()) {
        ReadResult result;
        result.key = key;

        uint64_t entry_version;
        Status s = getByVersion(key, snapshot_version, result.value, entry_version);
        result.version = snapshot_version;
        result.status = s;
        batch.addResult(std::move(result));
    }

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
    s = write_buffer_->flush(*seg, current_segment_id_, global_index_->index());
    if (!s.ok()) {
        return s;
    }

    current_segment_id_ = storage_->allocateSegmentId();
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
