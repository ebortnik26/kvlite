#ifndef KVLITE_DB_H
#define KVLITE_DB_H

#include <cstdint>
#include <memory>
#include <string>

#include "kvlite/status.h"
#include "kvlite/options.h"
#include "kvlite/write_batch.h"
#include "kvlite/read_batch.h"

namespace kvlite {

// Forward declarations
class DBImpl;
class SnapshotImpl;
class IteratorImpl;

class DB {
public:
    // --- Nested Classes ---

    // A Snapshot represents a consistent point-in-time view of the database.
    // All reads through a snapshot see data as of the snapshot's version.
    // Must be released with DB::releaseSnapshot() to allow GC of old versions.
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
        Snapshot(DB* db, uint64_t version);
        std::unique_ptr<SnapshotImpl> impl_;
    };

    // Unordered whole-database iterator.
    // Returns the latest version of each key as of the snapshot taken at creation.
    // Keys are returned in arbitrary order (not sorted).
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
        // Returns OK on success, NotFound when exhausted.
        Status next(std::string& key, std::string& value);

        // Advance to next entry and retrieve key/value/version.
        Status next(std::string& key, std::string& value, uint64_t& version);

        // Get the snapshot version this iterator uses.
        uint64_t snapshotVersion() const;

    private:
        friend class DB;
        explicit Iterator(std::unique_ptr<IteratorImpl> impl);
        std::unique_ptr<IteratorImpl> impl_;
    };

    // --- DB Methods ---

    DB();
    ~DB();

    // Non-copyable
    DB(const DB&) = delete;
    DB& operator=(const DB&) = delete;

    // Movable
    DB(DB&& other) noexcept;
    DB& operator=(DB&& other) noexcept;

    // --- Lifecycle ---

    // Open a database at the specified path
    Status open(const std::string& path, const Options& options = Options());

    // Close the database
    Status close();

    // Check if the database is open
    bool isOpen() const;

    // --- Point Operations ---

    // Set the value for the given key
    Status put(const std::string& key, const std::string& value,
               const WriteOptions& options = WriteOptions());

    // Get the latest value for the given key
    Status get(const std::string& key, std::string& value,
               const ReadOptions& options = ReadOptions());

    // Get the latest value and its version
    Status get(const std::string& key, std::string& value, uint64_t& version,
               const ReadOptions& options = ReadOptions());

    // Get the value at a specific version (largest version < upper_bound)
    Status getByVersion(const std::string& key, uint64_t upper_bound,
                        std::string& value,
                        const ReadOptions& options = ReadOptions());

    // Get the value at a specific version with version info
    Status getByVersion(const std::string& key, uint64_t upper_bound,
                        std::string& value, uint64_t& entry_version,
                        const ReadOptions& options = ReadOptions());

    // Remove the entry for the given key
    Status remove(const std::string& key,
                  const WriteOptions& options = WriteOptions());

    // Check if a key exists (latest version)
    Status exists(const std::string& key, bool& exists,
                  const ReadOptions& options = ReadOptions());

    // --- Batch Operations ---

    // Apply a batch of writes atomically (all ops get same version)
    Status write(const WriteBatch& batch,
                 const WriteOptions& options = WriteOptions());

    // Execute a batch of reads at a consistent snapshot
    Status read(ReadBatch& batch,
                const ReadOptions& options = ReadOptions());

    // --- Snapshots ---

    // Create a snapshot at the current version
    Status createSnapshot(std::unique_ptr<Snapshot>& snapshot);

    // Release a snapshot, allowing GC of versions it was protecting
    Status releaseSnapshot(std::unique_ptr<Snapshot> snapshot);

    // --- Iteration ---

    // Create an unordered iterator over all keys
    // Useful for full database copy (backup/replication)
    Status createIterator(std::unique_ptr<Iterator>& iterator);

    // --- Implementation Access ---

    // Returns internal implementation pointer.
    // DBImpl is forward-declared only; users cannot dereference this.
    // Used by DBAdmin to access administrative functionality.
    DBImpl* impl() const { return impl_.get(); }

private:
    std::unique_ptr<DBImpl> impl_;
};

} // namespace kvlite

#endif // KVLITE_DB_H
