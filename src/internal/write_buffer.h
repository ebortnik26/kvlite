#ifndef KVLITE_INTERNAL_WRITE_BUFFER_H
#define KVLITE_INTERNAL_WRITE_BUFFER_H

#include <cstdint>
#include <string>
#include <unordered_map>

namespace kvlite {
namespace internal {

// In-memory buffer for pending writes before flush to log files.
//
// Stores the latest write for each key. On flush, entries are written
// to the log file and the buffer is cleared.
//
// Thread-safety: Concurrency is managed at per-bucket level (not shown here).
class WriteBuffer {
public:
    struct Entry {
        uint64_t version;
        std::string value;
        bool tombstone;
    };

    WriteBuffer() = default;
    ~WriteBuffer() = default;

    // Insert or update an entry
    void put(const std::string& key, uint64_t version,
             const std::string& value, bool tombstone);

    // Get the entry for a key if present
    // Returns true if found, false if not in buffer
    bool get(const std::string& key, Entry& entry) const;

    // Check if key exists in buffer
    bool contains(const std::string& key) const;

    // Remove an entry (used after flush)
    void remove(const std::string& key);

    // Clear all entries
    void clear();

    // Get all entries for flushing
    const std::unordered_map<std::string, Entry>& entries() const {
        return buffer_;
    }

    // Statistics
    size_t size() const { return buffer_.size(); }
    size_t memoryUsage() const;
    bool empty() const { return buffer_.empty(); }

private:
    std::unordered_map<std::string, Entry> buffer_;
};

} // namespace internal
} // namespace kvlite

#endif // KVLITE_INTERNAL_WRITE_BUFFER_H
