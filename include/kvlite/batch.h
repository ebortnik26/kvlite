#ifndef KVLITE_BATCH_H
#define KVLITE_BATCH_H

#include <cstdint>
#include <string>
#include <vector>

#include "kvlite/status.h"

namespace kvlite {

// WriteBatch: append-only list of operations to apply atomically.
// All operations in a batch get the same version.
class WriteBatch {
public:
    enum class OpType : uint8_t {
        kPut = 0x01,
        kDelete = 0x02
    };

    struct Operation {
        OpType type;
        std::string key;
        std::string value;
    };

    void put(const std::string& key, const std::string& value) {
        operations_.push_back({OpType::kPut, key, value});
    }

    const std::vector<Operation>& operations() const { return operations_; }
    bool empty() const { return operations_.empty(); }

private:
    std::vector<Operation> operations_;
};

// Result of a single read operation in a ReadBatch.
struct ReadResult {
    std::string key;
    std::string value;
    uint64_t version = 0;
    Status status;
};

// ReadBatch: append-only list of keys to read at a consistent snapshot.
class ReadBatch {
public:
    void get(const std::string& key) { keys_.push_back(key); }

    const std::vector<std::string>& keys() const { return keys_; }
    const std::vector<ReadResult>& results() const { return results_; }
    bool empty() const { return keys_.empty(); }
    uint64_t snapshotVersion() const { return snapshot_version_; }

    // Output setters (used by DB::read implementation)
    void reserveResults(size_t count) { results_.reserve(count); }
    void addResult(ReadResult result) { results_.push_back(std::move(result)); }
    void setSnapshotVersion(uint64_t version) { snapshot_version_ = version; }
    void clearResults() { results_.clear(); snapshot_version_ = 0; }

private:
    std::vector<std::string> keys_;
    std::vector<ReadResult> results_;
    uint64_t snapshot_version_ = 0;
};

}  // namespace kvlite

#endif  // KVLITE_BATCH_H
