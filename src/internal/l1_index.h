#ifndef KVLITE_INTERNAL_L1_INDEX_H
#define KVLITE_INTERNAL_L1_INDEX_H

#include <cstdint>
#include <string>
#include <vector>
#include <functional>

#include "kvlite/status.h"
#include "log_entry.h"
#include "delta_hash_table.h"

namespace kvlite {
namespace internal {

// L1 Index: In-memory index mapping keys to file locations.
//
// Structure: key → [(version₁, file_id₁), (version₂, file_id₂), ...]
//            sorted by version, ascending
//
// The L1 index is always fully loaded in memory. It is persisted via:
// 1. WAL (append-only delta log for crash recovery)
// 2. Periodic snapshots (full dump every N updates + on shutdown)
//
// Thread-safety: Concurrency is managed at per-bucket level (not shown here).
class L1Index {
public:
    L1Index();
    ~L1Index();

    // Insert a new entry for a key
    // Entries are kept sorted by version (ascending)
    void put(const std::string& key, uint64_t version, uint32_t file_id);

    // Get the file_id for a key at a specific version
    // Returns the entry with largest version < upper_bound
    // Returns false if no such entry exists
    bool get(const std::string& key, uint64_t upper_bound,
             uint32_t& file_id, uint64_t& version) const;

    // Get the latest entry for a key
    // Returns false if key doesn't exist
    bool getLatest(const std::string& key,
                   uint32_t& file_id, uint64_t& version) const;

    // Check if a key exists (has any version)
    bool contains(const std::string& key) const;

    // Remove all entries for a key (used during GC compaction)
    void remove(const std::string& key);

    // Remove entries for a key where version < threshold (GC cleanup)
    void removeOldVersions(const std::string& key, uint64_t threshold);

    // Get all entries for a key (for debugging/testing)
    std::vector<IndexEntry> getEntries(const std::string& key) const;

    // Iterate over all keys and their entries
    // Callback receives (key, entries) for each key
    void forEach(const std::function<void(const std::string&,
                                          const std::vector<IndexEntry>&)>& fn) const;

    // Iterate over all keys (without entries, for efficiency)
    void forEachKey(const std::function<void(const std::string&)>& fn) const;

    // Get statistics
    size_t keyCount() const;
    size_t entryCount() const;
    size_t memoryUsage() const;

    // Clear all entries
    void clear();

    // --- Persistence ---

    // Save full snapshot to file
    Status saveSnapshot(const std::string& path) const;

    // Load snapshot from file
    Status loadSnapshot(const std::string& path);

    // Snapshot file format:
    // [magic: 4 bytes]["L1IX"]
    // [version: 4 bytes][1]
    // [num_keys: 8 bytes]
    // For each key:
    //   [key_len: 4 bytes][key: var]
    //   [num_entries: 4 bytes]
    //   For each entry:
    //     [version: 8 bytes][file_id: 4 bytes]
    // [checksum: 4 bytes]

private:
    DeltaHashTable dht_;
    size_t total_entries_ = 0;
};

} // namespace internal
} // namespace kvlite

#endif // KVLITE_INTERNAL_L1_INDEX_H
