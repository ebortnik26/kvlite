#ifndef KVLITE_WRITE_BATCH_H
#define KVLITE_WRITE_BATCH_H

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace kvlite {

// WriteBatch holds a collection of updates to apply atomically
// All operations in a batch are written to the same log file
class WriteBatch {
public:
    WriteBatch() = default;
    ~WriteBatch() = default;

    // Copyable and movable
    WriteBatch(const WriteBatch&) = default;
    WriteBatch& operator=(const WriteBatch&) = default;
    WriteBatch(WriteBatch&&) = default;
    WriteBatch& operator=(WriteBatch&&) = default;

    // Store the mapping "key -> value" in the database
    void put(const std::string& key, const std::string& value);

    // If the database contains a mapping for "key", erase it
    // Writes a tombstone marker
    void remove(const std::string& key);

    // Clear all updates buffered in this batch
    void clear();

    // Return the number of entries in the batch
    size_t count() const { return operations_.size(); }

    // Returns true if the batch contains no operations
    bool empty() const { return operations_.empty(); }

    // Approximate size of the batch in bytes (for buffer management)
    size_t approximateSize() const { return approximate_size_; }

    // Operation type
    enum class OpType : uint8_t {
        kPut = 0x01,
        kDelete = 0x02
    };

    // Single operation in the batch
    struct Operation {
        OpType type;
        std::string key;
        std::string value;  // Empty for delete operations
    };

    // Access operations (for internal use)
    const std::vector<Operation>& operations() const { return operations_; }

private:
    std::vector<Operation> operations_;
    size_t approximate_size_ = 0;
};

} // namespace kvlite

#endif // KVLITE_WRITE_BATCH_H
