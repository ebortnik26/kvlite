#ifndef KVLITE_BATCH_H
#define KVLITE_BATCH_H

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "kvlite/status.h"

namespace kvlite {

// WriteBatch holds a collection of updates to apply atomically.
// All operations in a batch get the same version.
class WriteBatch {
public:
    WriteBatch() = default;
    ~WriteBatch() = default;

    WriteBatch(const WriteBatch&) = default;
    WriteBatch& operator=(const WriteBatch&) = default;
    WriteBatch(WriteBatch&&) = default;
    WriteBatch& operator=(WriteBatch&&) = default;

    void put(const std::string& key, const std::string& value);
    void remove(const std::string& key);
    void clear();

    size_t count() const { return operations_.size(); }
    bool empty() const { return operations_.empty(); }
    size_t approximateSize() const { return approximate_size_; }

    enum class OpType : uint8_t {
        kPut = 0x01,
        kDelete = 0x02
    };

    struct Operation {
        OpType type;
        std::string key;
        std::string value;
    };

    const std::vector<Operation>& operations() const { return operations_; }

private:
    std::vector<Operation> operations_;
    size_t approximate_size_ = 0;
};

// Result of a single read operation in a ReadBatch.
struct ReadResult {
    std::string key;
    std::string value;
    uint64_t version = 0;
    Status status;

    bool ok() const { return status.ok(); }
    bool notFound() const { return status.isNotFound(); }
};

// ReadBatch performs multiple gets at a consistent snapshot.
class ReadBatch {
public:
    ReadBatch() = default;
    ~ReadBatch() = default;

    ReadBatch(const ReadBatch&) = default;
    ReadBatch& operator=(const ReadBatch&) = default;
    ReadBatch(ReadBatch&&) = default;
    ReadBatch& operator=(ReadBatch&&) = default;

    void get(const std::string& key);
    void get(const std::vector<std::string>& keys);
    void clear();

    const std::vector<std::string>& keys() const { return keys_; }
    const std::vector<ReadResult>& results() const { return results_; }

    const ReadResult* result(size_t index) const {
        return index < results_.size() ? &results_[index] : nullptr;
    }

    size_t count() const { return keys_.size(); }
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
