#include "kvlite/db.h"

#include <algorithm>

#include "internal/version_manager.h"
#include "internal/global_index_manager.h"
#include "internal/storage_manager.h"
#include "internal/log_entry.h"
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
    return db_->getByVersion(key, version_ + 1, value, entry_version, options);
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
        file_ids_ = db_->storage_->getFileIds();
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
        current_seg_index_ = db_->storage_->getSegmentIndex(current_file_id_);

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
            internal::LogEntry log_entry;
            Status s = db_->storage_->readLogEntry(current_file_id_, entry.offset, log_entry);
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
    internal::SegmentIndex* current_seg_index_ = nullptr;

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
    if (versions_ || global_index_ || storage_) {
        return Status::InvalidArgument("Database already open");
    }

    db_path_ = path;
    options_ = options;

    // Initialize version manager
    versions_ = std::make_unique<internal::VersionManager>();
    internal::VersionManager::Options ver_opts;
    // ver_opts.version_jump = options.version_jump;  // if configurable

    Status s = versions_->open(path, ver_opts);
    if (!s.ok()) {
        versions_.reset();
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
        global_index_.reset();
        return s;
    }

    s = global_index_->recover();
    if (!s.ok()) {
        versions_->close();
        global_index_->close();
        versions_.reset();
        global_index_.reset();
        return s;
    }

    // Initialize storage manager
    storage_ = std::make_unique<internal::StorageManager>();
    internal::StorageManager::Options storage_opts;
    storage_opts.log_file_size = options.log_file_size;
    storage_opts.write_buffer_size = options.write_buffer_size;
    storage_opts.segment_index_cache_size = options.segment_index_cache_size;
    storage_opts.gc_policy = options.gc_policy;
    storage_opts.gc_threshold = options.gc_threshold;
    storage_opts.gc_max_files = options.gc_max_files;
    storage_opts.sync_writes = options.sync_writes;
    storage_opts.verify_checksums = options.verify_checksums;

    auto oldest_version_fn = [this]() {
        return versions_->oldestSnapshotVersion();
    };

    s = storage_->open(path, storage_opts, oldest_version_fn);
    if (!s.ok()) {
        versions_->close();
        global_index_->close();
        versions_.reset();
        global_index_.reset();
        storage_.reset();
        return s;
    }

    s = storage_->recover();
    if (!s.ok()) {
        versions_->close();
        global_index_->close();
        storage_->close();
        versions_.reset();
        global_index_.reset();
        storage_.reset();
        return s;
    }

    return Status::OK();
}

Status DB::close() {
    if (!versions_ && !global_index_ && !storage_) {
        return Status::OK();
    }

    Status s1, s2, s3, s4;

    if (storage_) {
        s1 = storage_->flush();
        s2 = storage_->close();
        storage_.reset();
    }

    if (global_index_) {
        s3 = global_index_->close();
        global_index_.reset();
    }

    if (versions_) {
        s4 = versions_->close();
        versions_.reset();
    }

    if (!s1.ok()) return s1;
    if (!s2.ok()) return s2;
    if (!s3.ok()) return s3;
    return s4;
}

bool DB::isOpen() const {
    return versions_ != nullptr && global_index_ != nullptr && storage_ != nullptr;
}


// --- Point Operations ---

Status DB::put(const std::string& key, const std::string& value,
               const WriteOptions& options) {
    if (!isOpen()) {
        return Status::InvalidArgument("Database not open");
    }

    uint64_t version = versions_->allocateVersion();

    uint32_t segment_id;
    Status s = storage_->writeEntry(key, value, version, false, segment_id);
    if (!s.ok()) {
        return s;
    }

    s = global_index_->put(key, version, segment_id);
    if (!s.ok()) {
        return s;
    }

    if (options.sync) {
        return storage_->sync();
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

    std::vector<uint32_t> segment_ids;
    std::vector<uint64_t> versions;
    if (!global_index_->get(key, segment_ids, versions)) {
        return Status::NotFound(key);
    }

    // Iterate segment_ids (latest first). On fingerprint collision,
    // readValue returns NotFound for non-matching keys; try the next.
    for (uint32_t sid : segment_ids) {
        Status s = storage_->readValue(sid, key, version, value);
        if (s.ok()) {
            return s;
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

    std::vector<uint32_t> segment_ids;
    std::vector<uint64_t> versions;
    if (!global_index_->get(key, segment_ids, versions)) {
        return Status::NotFound(key);
    }

    // segment_ids are latest-first; scan each segment's SegmentIndex for the version
    for (uint32_t sid : segment_ids) {
        internal::SegmentIndex* seg_idx = storage_->getSegmentIndex(sid);
        if (!seg_idx) continue;
        uint64_t offset;
        if (seg_idx->get(key, upper_bound, offset, entry_version)) {
            return storage_->readValue(sid, key, entry_version, value);
        }
    }
    return Status::NotFound(key);
}

Status DB::remove(const std::string& key, const WriteOptions& options) {
    if (!isOpen()) {
        return Status::InvalidArgument("Database not open");
    }

    uint64_t version = versions_->allocateVersion();

    uint32_t segment_id;
    Status s = storage_->writeEntry(key, "", version, true, segment_id);
    if (!s.ok()) {
        return s;
    }

    s = global_index_->put(key, version, segment_id);
    if (!s.ok()) {
        return s;
    }

    if (options.sync) {
        return storage_->sync();
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
        uint32_t segment_id;
        bool tombstone = (op.type == WriteBatch::OpType::kDelete);
        Status s = storage_->writeEntry(op.key, op.value, version, tombstone, segment_id);
        if (!s.ok()) {
            return s;
        }

        s = global_index_->put(op.key, static_cast<uint32_t>(version), segment_id);
        if (!s.ok()) {
            return s;
        }
    }

    if (options.sync) {
        return storage_->sync();
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

        Status s = getByVersion(key, snapshot_version + 1, result.value, result.version);
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
    return storage_->flush();
}

// --- Statistics ---

Status DB::getStats(DBStats& stats) const {
    if (!isOpen()) {
        return Status::InvalidArgument("Database not open");
    }

    stats.num_log_files = storage_->numLogFiles();
    stats.total_log_size = storage_->totalLogSize();
    stats.global_index_size = global_index_->memoryUsage();
    stats.segment_index_cache_size = storage_->segmentIndexCacheSize();
    stats.segment_index_cached_count = storage_->segmentIndexCachedCount();
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
