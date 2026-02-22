#ifndef KVLITE_INTERNAL_STORAGE_MANAGER_H
#define KVLITE_INTERNAL_STORAGE_MANAGER_H

#include <atomic>
#include <cstdint>
#include <shared_mutex>
#include <string>
#include <map>
#include <vector>

#include "internal/segment.h"
#include "kvlite/status.h"

namespace kvlite {
namespace internal {

class Manifest;

// Storage Manager: Segment registry that owns the segment lifecycle.
//
// Provides:
// - Segment lifecycle: add, remove, lookup by ID
// - Segment ID allocation (monotonically increasing)
// - File path generation from segment ID
//
// Thread-safety: All public methods are thread-safe.
class StorageManager {
public:
    StorageManager();
    ~StorageManager();

    // Non-copyable
    StorageManager(const StorageManager&) = delete;
    StorageManager& operator=(const StorageManager&) = delete;

    // --- Lifecycle ---

    // Open storage at the given path.
    Status open(const std::string& db_path, Manifest& manifest);

    // Recover storage state from disk.
    Status recover();

    // Close storage.
    Status close();

    bool isOpen() const;

    // --- Segment Registry ---

    void addSegment(uint32_t id, Segment& segment);
    void removeSegment(uint32_t id);

    Segment* getSegment(uint32_t id);
    const Segment* getSegment(uint32_t id) const;
    
    std::vector<uint32_t> getSegmentIds() const;
    
    size_t segmentCount() const;
    uint64_t totalDataSize() const;

    // --- Segment Factory ---

    // Allocate a monotonically increasing segment ID.
    uint32_t allocateSegmentId();

    // Return the file path for a given segment ID under db_path_.
    std::string segmentPath(uint32_t segment_id) const;

private:
    std::string db_path_;
    Manifest* manifest_ = nullptr;
    bool is_open_ = false;
    std::atomic<uint32_t> next_segment_id_{1};

    mutable std::shared_mutex mutex_;
    std::map<uint32_t, Segment> segments_;
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_STORAGE_MANAGER_H
