#include "internal/version_manager.h"

#include <cstring>
#include <string>

#include "internal/manifest.h"

namespace kvlite {
namespace internal {

static constexpr char kNextVersionIdKey[] = "next_version_id";

VersionManager::VersionManager() = default;

VersionManager::~VersionManager() {
    if (is_open_) {
        close();
    }
}

Status VersionManager::open(const Options& options, Manifest& manifest) {
    if (is_open_) {
        return Status::InvalidArgument("VersionManager already open");
    }

    options_ = options;
    manifest_ = &manifest;
    is_open_ = true;
    return Status::OK();
}

Status VersionManager::recover() {
    // Read persisted counter from manifest.
    std::string val;
    if (manifest_->get(kNextVersionIdKey, val)) {
        persisted_counter_ = std::stoull(val);
        current_version_.store(persisted_counter_, std::memory_order_relaxed);
    } else {
        // Fresh database.
        current_version_.store(0, std::memory_order_relaxed);
        persisted_counter_ = 0;
    }
    return Status::OK();
}

Status VersionManager::close() {
    if (!is_open_) {
        return Status::OK();
    }

    // Persist final counter.
    uint64_t current = current_version_.load(std::memory_order_acquire);
    Status s = manifest_->set(kNextVersionIdKey, std::to_string(current));
    is_open_ = false;
    active_snapshots_.clear();
    manifest_ = nullptr;
    return s;
}

bool VersionManager::isOpen() const {
    return is_open_;
}

uint64_t VersionManager::allocateVersion() {
    uint64_t ver = current_version_.fetch_add(1, std::memory_order_acq_rel) + 1;

    // Persist when crossing block boundaries.
    if (ver > persisted_counter_) {
        std::lock_guard<std::mutex> lock(persist_mutex_);
        // Re-check under lock (another thread may have persisted).
        if (ver > persisted_counter_) {
            uint64_t new_persisted = persisted_counter_ + options_.block_size;
            manifest_->set(kNextVersionIdKey, std::to_string(new_persisted));
            persisted_counter_ = new_persisted;
        }
    }

    return ver;
}

uint64_t VersionManager::latestVersion() const {
    return current_version_.load(std::memory_order_acquire);
}

uint64_t VersionManager::createSnapshot() {
    uint64_t ver = current_version_.load(std::memory_order_acquire);
    std::lock_guard<std::mutex> lock(snapshot_mutex_);
    active_snapshots_.insert(ver);
    return ver;
}

void VersionManager::releaseSnapshot(uint64_t version) {
    std::lock_guard<std::mutex> lock(snapshot_mutex_);
    active_snapshots_.erase(version);
}

uint64_t VersionManager::oldestSnapshotVersion() const {
    std::lock_guard<std::mutex> lock(snapshot_mutex_);
    if (active_snapshots_.empty()) {
        return current_version_.load(std::memory_order_acquire);
    }
    return *active_snapshots_.begin();
}

size_t VersionManager::activeSnapshotCount() const {
    std::lock_guard<std::mutex> lock(snapshot_mutex_);
    return active_snapshots_.size();
}

}  // namespace internal
}  // namespace kvlite
