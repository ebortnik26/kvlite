#ifndef KVLITE_INTERNAL_L1_INDEX_H
#define KVLITE_INTERNAL_L1_INDEX_H

#include <cstdint>
#include <string>
#include <vector>
#include <functional>

#include "internal/l2_delta_hash_table.h"
#include "kvlite/status.h"

namespace kvlite {
namespace internal {

// L1 Index: In-memory index mapping keys to (segment_id, version) lists.
//
// Structure: key → [(segment_id₁, version₁), (segment_id₂, version₂), ...]
//            sorted by version desc (latest/highest first)
//
// Backed by L2DeltaHashTable with swapped field mapping:
//   DHT "offsets" → versions (sorted desc, so findFirst returns latest)
//   DHT "versions" → segment_ids (parallel)
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

    // Snapshot file format (v6, hash-based):
    // [magic: 4 bytes]["L1IX"]
    // [version: 4 bytes][6]
    // [num_entries: 8 bytes]
    // [key_count: 8 bytes]
    // Per entry (via forEach):
    //   [hash: 8 bytes]
    //   [version: 4 bytes]
    //   [segment_id: 4 bytes]
    // [checksum: 4 bytes]

private:
    L2DeltaHashTable dht_;
    size_t key_count_ = 0;
};

} // namespace internal
} // namespace kvlite

#endif // KVLITE_INTERNAL_L1_INDEX_H
