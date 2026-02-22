#ifndef KVLITE_INTERNAL_VERSION_MANAGER_H
#define KVLITE_INTERNAL_VERSION_MANAGER_H

#include <atomic>
#include <cstdint>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include "kvlite/status.h"

namespace kvlite {
namespace internal {

class Manifest;

// Version Manager: Manages version allocation and snapshot tracking.
//
// Responsibilities:
// - Allocate monotonically increasing versions
// - Persist version counter to MANIFEST (in configurable jumps)
// - Track active snapshots for GC safety
// - Provide oldest snapshot version for GC
//
// Persistence model:
// - Counter persisted in jumps (default 1M) to minimize I/O
// - On recovery, starts from persisted value (safe upper bound)
// - On close, persists final counter
//
// Thread-safety: All public methods are thread-safe.
class VersionManager {
public:
    struct Options {
        uint64_t version_jump = 1'000'000;
    };

    VersionManager();
    ~VersionManager();

    VersionManager(const VersionManager&) = delete;
    VersionManager& operator=(const VersionManager&) = delete;

    // --- Lifecycle ---

    Status open(const Options& options, Manifest& manifest);
    Status recover();
    Status close();
    bool isOpen() const;

    // --- Version Allocation ---

    // Allocate and return the next version.
    // Persists counter when crossing jump boundaries.
    uint64_t allocateVersion();

    // Get latest version (most recently allocated).
    uint64_t latestVersion() const;

    // --- Snapshot Management ---

    // Create a snapshot at the current version.
    // Returns the snapshot version.
    uint64_t createSnapshot();

    // Release a snapshot.
    void releaseSnapshot(uint64_t version);

    // Get the oldest active snapshot version.
    // Returns latestVersion() if no snapshots are active.
    uint64_t oldestSnapshotVersion() const;

    // Get number of active snapshots.
    size_t activeSnapshotCount() const;

    // Returns all observation points: active snapshot versions + latestVersion(),
    // sorted ascending.
    std::vector<uint64_t> snapshotVersions() const {
        std::lock_guard<std::mutex> lock(snapshot_mutex_);
        std::vector<uint64_t> result(active_snapshots_.begin(),
                                     active_snapshots_.end());
        uint64_t latest = current_version_.load(std::memory_order_acquire);
        // active_snapshots_ is a std::set, so result is already sorted ascending.
        // Append latestVersion() if it's not already the last element.
        if (result.empty() || result.back() < latest) {
            result.push_back(latest);
        }
        return result;
    }

private:
    Options options_;
    Manifest* manifest_ = nullptr;
    bool is_open_ = false;

    std::atomic<uint64_t> current_version_{0};
    uint64_t persisted_counter_{0};

    std::set<uint64_t> active_snapshots_;
    mutable std::mutex snapshot_mutex_;

    std::mutex persist_mutex_;
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_VERSION_MANAGER_H
