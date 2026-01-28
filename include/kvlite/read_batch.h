#ifndef KVLITE_READ_BATCH_H
#define KVLITE_READ_BATCH_H

#include <cstdint>
#include <string>
#include <vector>

#include "kvlite/status.h"

namespace kvlite {

// Result of a single read operation in a ReadBatch
struct ReadResult {
    std::string key;
    std::string value;
    uint64_t version = 0;
    Status status;        // OK, NotFound, or error

    bool ok() const { return status.ok(); }
    bool notFound() const { return status.isNotFound(); }
};

// ReadBatch performs multiple gets at a consistent snapshot.
//
// Internally creates a temporary snapshot, performs all reads,
// then releases the snapshot. This ensures all reads see a
// consistent point-in-time view of the database.
//
// Usage:
//   kvlite::ReadBatch batch;
//   batch.get("key1");
//   batch.get("key2");
//   batch.get("key3");
//
//   db.read(batch);  // All reads at same version
//
//   for (const auto& result : batch.results()) {
//       if (result.ok()) {
//           std::cout << result.key << " = " << result.value
//                     << " (v" << result.version << ")" << std::endl;
//       }
//   }
//
class ReadBatch {
public:
    ReadBatch() = default;
    ~ReadBatch() = default;

    // Copyable and movable
    ReadBatch(const ReadBatch&) = default;
    ReadBatch& operator=(const ReadBatch&) = default;
    ReadBatch(ReadBatch&&) = default;
    ReadBatch& operator=(ReadBatch&&) = default;

    // Add a key to be read
    void get(const std::string& key);

    // Add multiple keys to be read
    void get(const std::vector<std::string>& keys);

    // Clear all keys and results
    void clear();

    // Get the keys to be read
    const std::vector<std::string>& keys() const { return keys_; }

    // Get the results after executing the batch
    // Only valid after DB::read(batch) is called
    const std::vector<ReadResult>& results() const { return results_; }

    // Get a specific result by index
    const ReadResult& result(size_t index) const { return results_[index]; }

    // Number of keys in the batch
    size_t count() const { return keys_.size(); }

    // Returns true if the batch contains no keys
    bool empty() const { return keys_.empty(); }

    // Get the snapshot version used for reading
    // Only valid after DB::read(batch) is called
    uint64_t snapshotVersion() const { return snapshot_version_; }

private:
    friend class DB;
    friend class DBImpl;

    std::vector<std::string> keys_;
    std::vector<ReadResult> results_;
    uint64_t snapshot_version_ = 0;
};

} // namespace kvlite

#endif // KVLITE_READ_BATCH_H
