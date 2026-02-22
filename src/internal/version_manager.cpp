#include "internal/version_manager.h"

#include <cstring>
#include <fstream>
#include <sys/stat.h>

namespace kvlite {
namespace internal {

static constexpr char kManifestFile[] = "MANIFEST";
static constexpr char kMagic[4] = {'V', 'M', 'G', 'R'};
static constexpr uint32_t kFormatVersion = 1;

VersionManager::VersionManager() = default;

VersionManager::~VersionManager() {
    if (is_open_) {
        close();
    }
}

Status VersionManager::open(const std::string& db_path, const Options& options) {
    if (is_open_) {
        return Status::InvalidArgument("VersionManager already open");
    }

    db_path_ = db_path;
    options_ = options;

    // Ensure directory exists.
    ::mkdir(db_path.c_str(), 0755);

    // Try to read existing MANIFEST.
    Status s = readManifest();
    if (s.ok()) {
        // Start from persisted counter (safe upper bound).
        current_version_.store(persisted_counter_, std::memory_order_relaxed);
    } else if (s.isNotFound()) {
        // Fresh database.
        current_version_.store(0, std::memory_order_relaxed);
        persisted_counter_ = 0;
    } else {
        return s;
    }

    is_open_ = true;
    return Status::OK();
}

Status VersionManager::close() {
    if (!is_open_) {
        return Status::OK();
    }

    // Persist final counter.
    uint64_t current = current_version_.load(std::memory_order_acquire);
    Status s = writeManifest(current);
    is_open_ = false;
    active_snapshots_.clear();
    return s;
}

bool VersionManager::isOpen() const {
    return is_open_;
}

uint64_t VersionManager::allocateVersion() {
    uint64_t ver = current_version_.fetch_add(1, std::memory_order_acq_rel) + 1;

    // Persist when crossing jump boundaries.
    uint64_t next_boundary = persisted_counter_ + options_.version_jump;
    if (ver >= next_boundary) {
        std::lock_guard<std::mutex> lock(persist_mutex_);
        // Re-check under lock (another thread may have persisted).
        if (ver >= persisted_counter_ + options_.version_jump) {
            uint64_t new_persisted = ver + options_.version_jump;
            writeManifest(new_persisted);
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

// --- Persistence ---

std::string VersionManager::manifestPath() const {
    return db_path_ + "/" + kManifestFile;
}

Status VersionManager::writeManifest(uint64_t counter) {
    std::string path = manifestPath();
    std::ofstream file(path, std::ios::binary | std::ios::trunc);
    if (!file) {
        return Status::IOError("Failed to create MANIFEST: " + path);
    }

    file.write(kMagic, 4);
    file.write(reinterpret_cast<const char*>(&kFormatVersion), sizeof(kFormatVersion));
    file.write(reinterpret_cast<const char*>(&counter), sizeof(counter));

    if (!file.good()) {
        return Status::IOError("Failed to write MANIFEST: " + path);
    }
    return Status::OK();
}

Status VersionManager::readManifest() {
    std::string path = manifestPath();
    std::ifstream file(path, std::ios::binary);
    if (!file) {
        return Status::NotFound("MANIFEST not found: " + path);
    }

    char magic[4];
    file.read(magic, 4);
    if (!file.good() || std::memcmp(magic, kMagic, 4) != 0) {
        return Status::Corruption("Invalid MANIFEST magic");
    }

    uint32_t version;
    file.read(reinterpret_cast<char*>(&version), sizeof(version));
    if (!file.good() || version != kFormatVersion) {
        return Status::Corruption("Unsupported MANIFEST version");
    }

    uint64_t counter;
    file.read(reinterpret_cast<char*>(&counter), sizeof(counter));
    if (!file.good()) {
        return Status::Corruption("Failed to read MANIFEST counter");
    }

    persisted_counter_ = counter;
    return Status::OK();
}

}  // namespace internal
}  // namespace kvlite
