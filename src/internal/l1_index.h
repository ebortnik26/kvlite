#ifndef KVLITE_INTERNAL_L1_INDEX_H
#define KVLITE_INTERNAL_L1_INDEX_H

#include <cstdint>
#include <string>
#include <vector>
#include <functional>

#include "kvlite/status.h"
#include "delta_hash_table.h"

namespace kvlite {
namespace internal {

// L1 Index: In-memory index mapping keys to file_id lists.
//
// Structure: key → [file_id₁, file_id₂, ...]
//            reverse-sorted by version (latest first)
//
// Version resolution is delegated to L2 indexes.
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

    // Prepend file_id to key's list (if not already at front).
    // Latest file_id is always at index 0.
    void put(const std::string& key, uint32_t file_id);

    // Get all file_ids for a key. Returns nullptr if key doesn't exist.
    // File IDs are ordered latest-first.
    const std::vector<uint32_t>* getFileIds(const std::string& key) const;

    // Get the latest file_id for a key (index 0). O(1).
    // Returns false if key doesn't exist.
    bool getLatest(const std::string& key, uint32_t& file_id) const;

    // Check if a key exists (has any file_ids)
    bool contains(const std::string& key) const;

    // Remove all file_ids for a key (used during GC compaction)
    void remove(const std::string& key);

    // Remove a specific file_id from a key's list
    void removeFile(const std::string& key, uint32_t file_id);

    // Iterate over all keys and their file_ids
    // Callback receives (key, file_ids) for each key
    void forEach(const std::function<void(const std::string&,
                                          const std::vector<uint32_t>&)>& fn) const;

    // Iterate over all keys (without file_ids, for efficiency)
    void forEachKey(const std::function<void(const std::string&)>& fn) const;

    // Get statistics
    size_t keyCount() const;
    size_t entryCount() const;  // total file_id refs across all keys
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
    // [version: 4 bytes][2]
    // [num_keys: 8 bytes]
    // For each key:
    //   [key_len: 4 bytes][key: var]
    //   [num_file_ids: 4 bytes]
    //   For each file_id:
    //     [file_id: 4 bytes]
    // [checksum: 4 bytes]

private:
    DeltaHashTable dht_;
    size_t total_entries_ = 0;
};

} // namespace internal
} // namespace kvlite

#endif // KVLITE_INTERNAL_L1_INDEX_H
