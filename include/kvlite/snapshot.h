#ifndef KVLITE_SNAPSHOT_H
#define KVLITE_SNAPSHOT_H

#include <cstdint>
#include <memory>
#include <string>

#include "kvlite/status.h"
#include "kvlite/options.h"

namespace kvlite {

// Forward declarations
class DB;
class SnapshotImpl;

// A Snapshot represents a consistent point-in-time view of the database.
// All reads through a snapshot see data as of the snapshot's version.
//
// Snapshots must be explicitly released when no longer needed to allow
// garbage collection of old versions.
//
// Usage:
//   auto snapshot = db.createSnapshot();
//   std::string value;
//   snapshot->get("key", value);  // Reads at snapshot version
//   // ... more reads ...
//   db.releaseSnapshot(std::move(snapshot));  // Allow GC of old versions
//
class Snapshot {
public:
    ~Snapshot();

    // Non-copyable
    Snapshot(const Snapshot&) = delete;
    Snapshot& operator=(const Snapshot&) = delete;

    // Movable
    Snapshot(Snapshot&& other) noexcept;
    Snapshot& operator=(Snapshot&& other) noexcept;

    // Get the value for the given key as of this snapshot's version.
    // Equivalent to db.getByVersion(key, version_), but reusable.
    // Returns Status::NotFound if key does not exist at this version.
    Status get(const std::string& key, std::string& value,
               const ReadOptions& options = ReadOptions()) const;

    // Get the value and its version
    Status get(const std::string& key, std::string& value, uint64_t& entry_version,
               const ReadOptions& options = ReadOptions()) const;

    // Check if a key exists at this snapshot's version
    Status exists(const std::string& key, bool& exists,
                  const ReadOptions& options = ReadOptions()) const;

    // Get the version this snapshot represents
    uint64_t version() const;

    // Check if the snapshot is still valid (not released)
    bool isValid() const;

private:
    friend class DB;

    // Only DB can create snapshots
    Snapshot(DB* db, uint64_t version);

    std::unique_ptr<SnapshotImpl> impl_;
};

} // namespace kvlite

#endif // KVLITE_SNAPSHOT_H
