#ifndef KVLITE_INTERNAL_WRITE_BUFFER_H
#define KVLITE_INTERNAL_WRITE_BUFFER_H

#include <cstdint>
#include <string>
#include <vector>
#include <unordered_map>
#include <functional>

#include "log_entry.h"

namespace kvlite {
namespace internal {

// In-memory buffer for pending writes before flush to log files.
//
// Structure: key → [(version₁, value₁), (version₂, value₂), ...]
//            sorted by version, ascending
//
// Conceptually the same as an L2 file, but read-write.
// On flush, all entries are written to the log file and the buffer is cleared.
//
// Thread-safety: Concurrency is managed at per-bucket level (not shown here).
class WriteBuffer {
public:
    struct Entry {
        PackedVersion pv;
        std::string value;

        uint64_t version() const { return pv.version(); }
        bool tombstone() const { return pv.tombstone(); }

        bool operator<(const Entry& other) const { return pv < other.pv; }
    };

    WriteBuffer() = default;
    ~WriteBuffer() = default;

    // Append a new entry for a key
    void put(const std::string& key, uint64_t version,
             const std::string& value, bool tombstone);

    // Get the latest entry for a key
    // Returns true if found, false if not in buffer
    bool get(const std::string& key,
             std::string& value, uint64_t& version, bool& tombstone) const;

    // Get entry for a key with largest version < upper_bound
    // Returns true if found, false if not in buffer
    bool getByVersion(const std::string& key, uint64_t upper_bound,
                      std::string& value, uint64_t& version, bool& tombstone) const;

    // --- Flush support ---

    // Iterate over all entries (for flush)
    void forEach(const std::function<void(const std::string&,
                                          const std::vector<Entry>&)>& fn) const;

    // Clear all entries
    void clear();

    // Statistics
    size_t keyCount() const { return buffer_.size(); }
    size_t entryCount() const;
    size_t memoryUsage() const;
    bool empty() const { return buffer_.empty(); }

private:
    // Key -> list of (version, value, tombstone), sorted by version ascending
    std::unordered_map<std::string, std::vector<Entry>> buffer_;
};

} // namespace internal
} // namespace kvlite

#endif // KVLITE_INTERNAL_WRITE_BUFFER_H
