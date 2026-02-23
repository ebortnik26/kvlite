#include "internal/global_index_manager.h"

#include "internal/global_index.h"
#include "internal/global_index_wal.h"

namespace kvlite {
namespace internal {

GlobalIndexManager::GlobalIndexManager() = default;

GlobalIndexManager::~GlobalIndexManager() {
    if (is_open_) {
        close();
    }
}

// --- Lifecycle ---

Status GlobalIndexManager::open(const std::string& db_path, const Options& options) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (is_open_) {
        return Status::InvalidArgument("Already open");
    }

    db_path_ = db_path;
    options_ = options;
    index_ = std::make_unique<GlobalIndex>();
    is_open_ = true;
    return Status::OK();
}

Status GlobalIndexManager::recover() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::string path = snapshotPath();
    Status s = index_->loadSnapshot(path);
    if (s.ok()) {
        return Status::OK();
    }
    // No snapshot or corrupted — start with empty index.
    // Caller (DB::open) will rebuild from segments if needed.
    return Status::OK();
}

Status GlobalIndexManager::close() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!is_open_) {
        return Status::OK();
    }
    // Persist index to snapshot before closing.
    if (index_) {
        Status s = index_->saveSnapshot(snapshotPath());
        if (!s.ok()) {
            index_.reset();
            wal_.reset();
            is_open_ = false;
            return s;
        }
    }
    index_.reset();
    wal_.reset();
    is_open_ = false;
    return Status::OK();
}

bool GlobalIndexManager::isOpen() const {
    return is_open_;
}

// --- Index Operations ---

Status GlobalIndexManager::put(const std::string& key, uint64_t version,
                                uint32_t segment_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    index_->put(key, version, segment_id);
    updates_since_snapshot_++;
    return Status::OK();
}

bool GlobalIndexManager::get(const std::string& key,
                              std::vector<uint32_t>& segment_ids,
                              std::vector<uint64_t>& versions) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return index_->get(key, segment_ids, versions);
}

bool GlobalIndexManager::get(const std::string& key, uint64_t upper_bound,
                              uint64_t& version, uint32_t& segment_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return index_->get(key, upper_bound, version, segment_id);
}

Status GlobalIndexManager::getLatest(const std::string& key,
                                      uint64_t& version,
                                      uint32_t& segment_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (index_->getLatest(key, version, segment_id)) {
        return Status::OK();
    }
    return Status::NotFound(key);
}

bool GlobalIndexManager::contains(const std::string& key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return index_->contains(key);
}

Status GlobalIndexManager::remove(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    index_->remove(key);
    updates_since_snapshot_++;
    return Status::OK();
}

void GlobalIndexManager::removeSegment(const std::string& key,
                                        uint32_t segment_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    index_->removeSegment(key, segment_id);
}

// --- Maintenance ---

Status GlobalIndexManager::snapshot() {
    // Stub: no persistence yet.
    std::lock_guard<std::mutex> lock(mutex_);
    updates_since_snapshot_ = 0;
    return Status::OK();
}

Status GlobalIndexManager::sync() {
    // Stub: no WAL yet.
    return Status::OK();
}

// --- Statistics ---

size_t GlobalIndexManager::keyCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return index_ ? index_->keyCount() : 0;
}

size_t GlobalIndexManager::entryCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return index_ ? index_->entryCount() : 0;
}

size_t GlobalIndexManager::memoryUsage() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return index_ ? index_->memoryUsage() : 0;
}

uint64_t GlobalIndexManager::updatesSinceSnapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return updates_since_snapshot_;
}

// --- Private ---

Status GlobalIndexManager::maybeSnapshot() {
    // Stub: no auto-snapshot yet.
    return Status::OK();
}

std::string GlobalIndexManager::snapshotPath() const {
    return db_path_ + "/global_index.snapshot";
}

std::string GlobalIndexManager::walPath() const {
    return db_path_ + "/global_index.wal";
}

}  // namespace internal
}  // namespace kvlite
