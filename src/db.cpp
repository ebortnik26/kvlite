#include "kvlite/db.h"

#include <algorithm>

#include "internal/version_manager.h"
#include "internal/l1_index_manager.h"
#include "internal/storage_manager.h"
#include "internal/log_entry.h"
#include "internal/l2_index.h"

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
// Scans L2 files from newest to oldest. For each entry, consults L1 index
// to check if it's the latest version at the snapshot. Only returns entries
// that are the current (non-tombstone) value for their key.

class DB::Iterator::Impl {
public:
    Impl(DB* db, uint64_t snapshot_version)
        : db_(db), snapshot_version_(snapshot_version) {
        // Get file IDs sorted by file_id (ascending = oldest to newest)
        file_ids_ = db_->storage_->getFileIds();
        // Reverse to iterate newest to oldest
        std::reverse(file_ids_.begin(), file_ids_.end());

        if (!file_ids_.empty()) {
            current_file_idx_ = 0;
            loadCurrentFile();
            findNextValid();
        }
    }

    ~Impl() {
        db_->versions_->releaseSnapshot(snapshot_version_);
    }

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

    uint64_t snapshotVersion() const { return snapshot_version_; }

private:
    void loadCurrentFile() {
        if (current_file_idx_ >= file_ids_.size()) {
            current_l2_ = nullptr;
            l2_entries_.clear();
            l2_entry_idx_ = 0;
            return;
        }

        uint32_t file_id = file_ids_[current_file_idx_];
        current_l2_ = db_->storage_->getL2Index(file_id);

        // Collect all entries from this L2 index
        l2_entries_.clear();
        if (current_l2_) {
            current_l2_->forEach([this, file_id](const std::string& key,
                                                  const std::vector<internal::IndexEntry>& entries) {
                for (const auto& entry : entries) {
                    // Only consider entries within our snapshot
                    if (entry.version <= snapshot_version_) {
                        L2Entry e;
                        e.key = key;
                        e.version = entry.version;
                        e.offset = entry.location;
                        e.file_id = file_id;
                        l2_entries_.push_back(std::move(e));
                    }
                }
            });
        }
        l2_entry_idx_ = 0;
    }

    void advanceAndFindNext() {
        l2_entry_idx_++;
        findNextValid();
    }

    void findNextValid() {
        while (true) {
            // Move to next file if needed
            while (l2_entry_idx_ >= l2_entries_.size()) {
                current_file_idx_++;
                if (current_file_idx_ >= file_ids_.size()) {
                    valid_ = false;
                    return;
                }
                loadCurrentFile();
            }

            const auto& entry = l2_entries_[l2_entry_idx_];

            // Skip if we've already returned this key
            if (seen_keys_.count(entry.key)) {
                l2_entry_idx_++;
                continue;
            }

            // Check L1 index: is this the latest version at our snapshot?
            uint32_t latest_file_id;
            uint64_t latest_version;
            Status s = db_->l1_index_->get(entry.key, snapshot_version_ + 1,
                                           latest_file_id, latest_version);

            if (!s.ok()) {
                // Key doesn't exist at this snapshot (shouldn't happen, but skip)
                l2_entry_idx_++;
                continue;
            }

            if (entry.version != latest_version) {
                // Not the latest version, skip
                l2_entry_idx_++;
                continue;
            }

            // This is the latest version - read the actual entry
            internal::LogEntry log_entry;
            s = db_->storage_->readLogEntry(entry.file_id, entry.offset, log_entry);
            if (!s.ok()) {
                l2_entry_idx_++;
                continue;
            }

            // Skip tombstones
            if (log_entry.tombstone()) {
                seen_keys_.insert(entry.key);
                l2_entry_idx_++;
                continue;
            }

            // Found a valid entry
            seen_keys_.insert(entry.key);
            current_key_ = std::move(log_entry.key);
            current_value_ = std::move(log_entry.value);
            current_version_ = log_entry.version();
            valid_ = true;
            return;
        }
    }

    struct L2Entry {
        std::string key;
        uint64_t version;
        uint64_t offset;
        uint32_t file_id;
    };

    DB* db_;
    uint64_t snapshot_version_;

    std::vector<uint32_t> file_ids_;
    size_t current_file_idx_ = 0;
    internal::L2Index* current_l2_ = nullptr;

    std::vector<L2Entry> l2_entries_;
    size_t l2_entry_idx_ = 0;

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

uint64_t DB::Iterator::snapshotVersion() const {
    return impl_->snapshotVersion();
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
    if (versions_ || l1_index_ || storage_) {
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

    // Initialize L1 index manager
    l1_index_ = std::make_unique<internal::L1IndexManager>();
    internal::L1IndexManager::Options l1_opts;
    l1_opts.snapshot_interval = options.l1_snapshot_interval;
    l1_opts.sync_writes = options.sync_writes;

    s = l1_index_->open(path, l1_opts);
    if (!s.ok()) {
        versions_->close();
        versions_.reset();
        l1_index_.reset();
        return s;
    }

    s = l1_index_->recover();
    if (!s.ok()) {
        versions_->close();
        l1_index_->close();
        versions_.reset();
        l1_index_.reset();
        return s;
    }

    // Initialize storage manager
    storage_ = std::make_unique<internal::StorageManager>();
    internal::StorageManager::Options storage_opts;
    storage_opts.log_file_size = options.log_file_size;
    storage_opts.write_buffer_size = options.write_buffer_size;
    storage_opts.l2_cache_size = options.l2_cache_size;
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
        l1_index_->close();
        versions_.reset();
        l1_index_.reset();
        storage_.reset();
        return s;
    }

    s = storage_->recover();
    if (!s.ok()) {
        versions_->close();
        l1_index_->close();
        storage_->close();
        versions_.reset();
        l1_index_.reset();
        storage_.reset();
        return s;
    }

    return Status::OK();
}

Status DB::close() {
    if (!versions_ && !l1_index_ && !storage_) {
        return Status::OK();
    }

    Status s1, s2, s3, s4;

    if (storage_) {
        s1 = storage_->flush();
        s2 = storage_->close();
        storage_.reset();
    }

    if (l1_index_) {
        s3 = l1_index_->close();
        l1_index_.reset();
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
    return versions_ != nullptr && l1_index_ != nullptr && storage_ != nullptr;
}


// --- Point Operations ---

Status DB::put(const std::string& key, const std::string& value,
               const WriteOptions& options) {
    if (!isOpen()) {
        return Status::InvalidArgument("Database not open");
    }

    uint64_t version = versions_->allocateVersion();

    uint32_t file_id;
    Status s = storage_->writeEntry(key, value, version, false, file_id);
    if (!s.ok()) {
        return s;
    }

    s = l1_index_->put(key, version, file_id);
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

    uint32_t file_id;
    Status s = l1_index_->getLatest(key, file_id, version);
    if (!s.ok()) {
        return s;
    }

    return storage_->readValue(file_id, key, version, value);
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

    uint32_t file_id;
    Status s = l1_index_->get(key, upper_bound, file_id, entry_version);
    if (!s.ok()) {
        return s;
    }

    return storage_->readValue(file_id, key, entry_version, value);
}

Status DB::remove(const std::string& key, const WriteOptions& options) {
    if (!isOpen()) {
        return Status::InvalidArgument("Database not open");
    }

    uint64_t version = versions_->allocateVersion();

    uint32_t file_id;
    Status s = storage_->writeEntry(key, "", version, true, file_id);
    if (!s.ok()) {
        return s;
    }

    s = l1_index_->put(key, version, file_id);
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
        uint32_t file_id;
        bool tombstone = (op.type == WriteBatch::OpType::kDelete);
        Status s = storage_->writeEntry(op.key, op.value, version, tombstone, file_id);
        if (!s.ok()) {
            return s;
        }

        s = l1_index_->put(op.key, version, file_id);
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
    uint64_t snapshot_version = versions_->currentVersion();
    batch.setSnapshotVersion(snapshot_version);
    batch.reserveResults(batch.count());

    for (const auto& key : batch.keys()) {
        ReadResult result;
        result.key = key;

        uint32_t file_id;
        Status s = l1_index_->get(key, snapshot_version + 1, file_id, result.version);
        if (s.ok()) {
            s = storage_->readValue(file_id, key, result.version, result.value);
        }
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

    uint64_t snapshot_version = versions_->createSnapshot();
    auto impl = std::make_unique<Iterator::Impl>(this, snapshot_version);
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
    stats.l1_index_size = l1_index_->memoryUsage();
    stats.l2_cache_size = storage_->l2CacheSize();
    stats.l2_cached_count = storage_->l2CachedCount();
    stats.current_version = versions_->currentVersion();
    stats.oldest_version = versions_->oldestSnapshotVersion();

    stats.active_snapshots = versions_->activeSnapshotCount();

    // These would need L1 index iteration to compute accurately
    stats.num_live_entries = l1_index_->keyCount();
    stats.num_historical_entries = l1_index_->entryCount() - l1_index_->keyCount();

    return Status::OK();
}

Status DB::getPath(std::string& path) const {
    if (!isOpen()) {
        return Status::InvalidArgument("Database not open");
    }
    path = db_path_;
    return Status::OK();
}

uint64_t DB::getCurrentVersion() const {
    return versions_->currentVersion();
}

uint64_t DB::getOldestVersion() const {
    return versions_->oldestSnapshotVersion();
}

}  // namespace kvlite
