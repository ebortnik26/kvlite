#ifndef KVLITE_INTERNAL_L1_INDEX_H
#define KVLITE_INTERNAL_L1_INDEX_H

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>
#include <functional>

#include "kvlite/status.h"

namespace kvlite {
namespace internal {

// L1 Index: In-memory index mapping keys to (segment_id, version) lists.
//
// Structure: key → [(segment_id₁, version₁), (segment_id₂, version₂), ...]
//            sorted by version desc (latest/highest first)
//
// The L1 index is always fully loaded in memory. It is persisted via:
// 1. WAL (append-only delta log for crash recovery)
// 2. Periodic snapshots (full dump every N updates + on shutdown)
//
// Thread-safety: external synchronization required (L1IndexManager provides it).
class L1Index {
public:
    L1Index();
    ~L1Index();

    // Append (segment_id, version) to key's list.
    void put(const std::string& key, uint64_t version, uint32_t segment_id);

    // Get all (segment_id, version) pairs for a key. Returns false if key
    // doesn't exist. Pairs are ordered latest-first (highest version first).
    bool get(const std::string& key,
             std::vector<uint32_t>& segment_ids,
             std::vector<uint64_t>& versions) const;

    // Get the latest entry for a key with version <= upper_bound.
    // Returns false if no matching entry exists.
    bool get(const std::string& key, uint64_t upper_bound,
             uint64_t& version, uint32_t& segment_id) const;

    // Get the latest (highest version) entry for a key.
    // Returns false if key doesn't exist.
    bool getLatest(const std::string& key,
                   uint64_t& version, uint32_t& segment_id) const;

    // Check if a key exists.
    bool contains(const std::string& key) const;

    // Remove all entries for a key (used during GC compaction).
    void remove(const std::string& key);

    // Remove all entries pointing to a specific segment_id from a key's list.
    void removeSegment(const std::string& key, uint32_t segment_id);

    // Get statistics.
    size_t keyCount() const;
    size_t entryCount() const;  // total (segment_id, version) refs across all keys
    size_t memoryUsage() const;

    // Clear all entries.
    void clear();

    // --- Persistence ---

    // Save full snapshot to file.
    Status saveSnapshot(const std::string& path) const;

    // Load snapshot from file.
    Status loadSnapshot(const std::string& path);

    // Snapshot file format (v5):
    // [magic: 4 bytes]["L1IX"]
    // [version: 4 bytes][5]
    // [num_keys: 8 bytes]
    // For each key:
    //   [key_len: 4 bytes]
    //   [key: key_len bytes]
    //   [num_entries: 4 bytes]
    //   For each entry:
    //     [segment_id: 4 bytes]
    //     [version: 8 bytes]
    // [checksum: 4 bytes]

private:
    struct Entry {
        uint32_t segment_id;
        uint64_t version;
    };

    std::unordered_map<std::string, std::vector<Entry>> index_;
    size_t key_count_ = 0;
    size_t entry_count_ = 0;
};

} // namespace internal
} // namespace kvlite

#endif // KVLITE_INTERNAL_L1_INDEX_H
