#ifndef KVLITE_WRITE_BATCH_H
#define KVLITE_WRITE_BATCH_H

#include <string>
#include <vector>

namespace kvlite {

class WriteBatch {
public:
    WriteBatch() = default;
    ~WriteBatch() = default;

    // Store the mapping "key -> value" in the database
    void put(const std::string& key, const std::string& value);

    // If the database contains a mapping for "key", erase it
    void remove(const std::string& key);

    // Clear all updates buffered in this batch
    void clear();

    // Return the number of entries in the batch
    size_t count() const { return operations_.size(); }

    // Returns true if the batch contains no operations
    bool empty() const { return operations_.empty(); }

    // Iteration support for internal use
    struct Operation {
        enum Type { kPut, kDelete };
        Type type;
        std::string key;
        std::string value;
    };

    const std::vector<Operation>& operations() const { return operations_; }

private:
    std::vector<Operation> operations_;
};

} // namespace kvlite

#endif // KVLITE_WRITE_BATCH_H
