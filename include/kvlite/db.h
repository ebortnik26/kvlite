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
class DBAdmin;
class Snapshot;

class DB {
public:
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
    // Creates the directory if options.create_if_missing is true
    Status open(const std::string& path, const Options& options = Options());

    // Close the database
    // Flushes write buffer, persists L1 snapshot, and releases resources
    Status close();

    // Check if the database is open
    bool isOpen() const;

    // --- Basic Operations ---

    // Set the value for the given key
    // Creates a new version; previous versions are retained until GC
    Status put(const std::string& key, const std::string& value,
               const WriteOptions& options = WriteOptions());

    // Get the latest value for the given key
    // Returns Status::NotFound if the key does not exist
    Status get(const std::string& key, std::string& value,
               const ReadOptions& options = ReadOptions());

    // Get the latest value and its version
    Status get(const std::string& key, std::string& value, uint64_t& version,
               const ReadOptions& options = ReadOptions());

    // Get the value at a specific version (point-in-time read)
    // Returns the latest version of key where version < upper_bound
    // Returns Status::NotFound if key did not exist at that version
    Status getByVersion(const std::string& key, uint64_t upper_bound,
                        std::string& value,
                        const ReadOptions& options = ReadOptions());

    // Get the value at a specific version with version info
    Status getByVersion(const std::string& key, uint64_t upper_bound,
                        std::string& value, uint64_t& entry_version,
                        const ReadOptions& options = ReadOptions());

    // Remove the entry for the given key
    // Writes a tombstone with a new version; actual deletion happens during GC
    Status remove(const std::string& key,
                  const WriteOptions& options = WriteOptions());

    // Check if a key exists (latest version)
    Status exists(const std::string& key, bool& exists,
                  const ReadOptions& options = ReadOptions());

    // --- Batch Operations ---

    // Apply a batch of writes atomically
    // All operations in the batch get the same version
    Status write(const WriteBatch& batch,
                 const WriteOptions& options = WriteOptions());

    // Execute a batch of reads at a consistent snapshot
    // Creates a temporary snapshot, performs all reads, then releases it
    // Results are stored in the batch object
    Status read(ReadBatch& batch,
                const ReadOptions& options = ReadOptions());

    // --- Snapshots ---

    // Create a snapshot at the current version
    // The snapshot provides a consistent point-in-time view
    // Must be released with releaseSnapshot() to allow GC
    Status createSnapshot(std::unique_ptr<Snapshot>& snapshot);

    // Release a snapshot, allowing GC of versions it was protecting
    Status releaseSnapshot(std::unique_ptr<Snapshot> snapshot);

private:
    friend class DBAdmin;
    std::unique_ptr<DBImpl> impl_;
};

} // namespace kvlite

#endif // KVLITE_DB_H
