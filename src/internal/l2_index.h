#ifndef KVLITE_INTERNAL_L2_INDEX_H
#define KVLITE_INTERNAL_L2_INDEX_H

#include <cstdint>
#include <string>
#include <vector>
#include <unordered_map>
#include <functional>

#include "kvlite/status.h"
#include "log_entry.h"

namespace kvlite {
namespace internal {

// L2 Index: Per-file index mapping keys to byte offsets within a log file.
//
// Structure: key → [(version₁, offset₁), (version₂, offset₂), ...]
//            sorted by version, ascending
//
// Each log file (log_NNNNNNNN.data) has a corresponding L2 index file
// (log_NNNNNNNN.idx). L2 indices are loaded on-demand and cached in an
// LRU cache.
//
// Thread-safety: Individual L2Index instances are not thread-safe.
// Thread-safety is provided by the L2Cache which manages access.
class L2Index {
public:
    L2Index();
    explicit L2Index(uint32_t file_id);
    ~L2Index();

    // Insert a new entry for a key
    void put(const std::string& key, uint64_t version, uint64_t offset);

    // Get the offset for a key at a specific version
    // Returns the entry with largest version < upper_bound
    // Returns false if no such entry exists
    bool get(const std::string& key, uint64_t upper_bound,
             uint64_t& offset, uint64_t& version) const;

    // Get the latest entry for a key
    // Returns false if key doesn't exist in this file
    bool getLatest(const std::string& key,
                   uint64_t& offset, uint64_t& version) const;

    // Check if a key exists in this index
    bool contains(const std::string& key) const;

    // Get all entries for a key
    std::vector<IndexEntry> getEntries(const std::string& key) const;

    // Iterate over all keys and their entries
    void forEach(const std::function<void(const std::string&,
                                          const std::vector<IndexEntry>&)>& fn) const;

    // Get file ID this index belongs to
    uint32_t fileId() const { return file_id_; }

    // Get statistics
    size_t keyCount() const { return index_.size(); }
    size_t entryCount() const;
    size_t memoryUsage() const;

    // Clear all entries
    void clear();

    // --- Persistence ---

    // Save index to file
    Status save(const std::string& path) const;

    // Load index from file
    Status load(const std::string& path);

    // Build index by scanning a log file
    Status buildFromLogFile(const std::string& log_path);

    // Index file format:
    // [magic: 4 bytes]["L2IX"]
    // [version: 4 bytes][1]
    // [file_id: 4 bytes]
    // [num_keys: 8 bytes]
    // For each key:
    //   [key_len: 4 bytes][key: var]
    //   [num_entries: 4 bytes]
    //   For each entry:
    //     [version: 8 bytes][offset: 8 bytes]
    // [checksum: 4 bytes]

private:
    uint32_t file_id_ = 0;

    // Key -> list of (version, offset), sorted by version ascending
    std::unordered_map<std::string, std::vector<IndexEntry>> index_;
};

} // namespace internal
} // namespace kvlite

#endif // KVLITE_INTERNAL_L2_INDEX_H
