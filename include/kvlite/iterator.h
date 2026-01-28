#ifndef KVLITE_ITERATOR_H
#define KVLITE_ITERATOR_H

#include <cstdint>
#include <memory>
#include <string>

#include "kvlite/status.h"
#include "kvlite/options.h"

namespace kvlite {

// Forward declarations
class DB;
class IteratorImpl;

// Unordered whole-database iterator.
//
// Iterates through all keys in the database, returning the latest version
// of each key as of the snapshot taken when the iterator was created.
// Keys are returned in arbitrary order (hash/storage order, not sorted).
//
// Implementation:
//   Scans L2 index files sequentially. For each (key, version, offset) entry:
//   1. Skip if version > snapshot_version
//   2. Check L1 index: is this the latest version for key where version <= snapshot?
//   3. If yes, read value from log file and emit
//   No additional in-memory index needed - L1 is already in memory.
//
// Usage:
//   std::unique_ptr<kvlite::Iterator> iter;
//   db.createIterator(iter);
//
//   std::string key, value;
//   while (iter->next(key, value).ok()) {
//       std::cout << key << " = " << value << std::endl;
//   }
//
// Note: The iterator holds an implicit snapshot. While the iterator exists,
// GC cannot collect versions >= the snapshot version.
//
class Iterator {
public:
    ~Iterator();

    // Non-copyable
    Iterator(const Iterator&) = delete;
    Iterator& operator=(const Iterator&) = delete;

    // Movable
    Iterator(Iterator&& other) noexcept;
    Iterator& operator=(Iterator&& other) noexcept;

    // Advance to next entry and retrieve key/value.
    // Returns Status::OK on success, Status::NotFound when exhausted,
    // or an error status on I/O failure.
    Status next(std::string& key, std::string& value);

    // Advance to next entry and retrieve key/value/version.
    Status next(std::string& key, std::string& value, uint64_t& version);

    // Get the snapshot version this iterator uses.
    // All reads are consistent as of this version.
    uint64_t snapshotVersion() const;

private:
    friend class DB;

    // Only DB can create iterators
    explicit Iterator(std::unique_ptr<IteratorImpl> impl);

    std::unique_ptr<IteratorImpl> impl_;
};

} // namespace kvlite

#endif // KVLITE_ITERATOR_H
