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

// Payload encoding for inline file_ids in 64-bit DHT payloads.
//
// Inline (bit 0 = 1):
//   bit 0:      1 (inline flag)
//   bit 1:      has_second (0 = 1 file_id, 1 = 2 file_ids)
//   bits 2-32:  file_id_0 (31 bits, max 2^31-1)
//   bits 33-63: file_id_1 (31 bits, valid only if has_second=1)
//
// Overflow (bit 0 = 0):
//   bits 0-63: (pool_handle + 1) << 1  (always non-zero, bit 0 = 0)
//   Pool handle is an index into IdVecPool.
namespace Payload {

static constexpr uint32_t kMaxInlineFileId = (1u << 31) - 1;

inline bool isInline(uint64_t p) { return (p & 1) != 0; }
inline bool isOverflow(uint64_t p) { return (p & 1) == 0; }

inline uint64_t makeInline1(uint32_t fid) {
    return 1ULL | (static_cast<uint64_t>(fid) << 2);
}

inline uint64_t makeInline2(uint32_t fid0, uint32_t fid1) {
    return 1ULL | (1ULL << 1) |
           (static_cast<uint64_t>(fid0) << 2) |
           (static_cast<uint64_t>(fid1) << 33);
}

inline uint32_t inlineCount(uint64_t p) {
    return ((p >> 1) & 1) ? 2 : 1;
}

inline uint32_t inlineFileId(uint64_t p, uint32_t idx) {
    if (idx == 0) return static_cast<uint32_t>((p >> 2) & kMaxInlineFileId);
    return static_cast<uint32_t>((p >> 33) & kMaxInlineFileId);
}

inline uint32_t toHandle(uint64_t p) {
    return static_cast<uint32_t>(p >> 1) - 1;
}

inline uint64_t fromHandle(uint32_t handle) {
    return static_cast<uint64_t>(handle + 1) << 1;
}

}  // namespace Payload

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

    // Get all file_ids for a key. Returns false if key doesn't exist.
    // File IDs are ordered latest-first.
    bool getFileIds(const std::string& key, std::vector<uint32_t>& out) const;

    // Get the latest file_id for a key (index 0). O(1).
    // Returns false if key doesn't exist.
    bool getLatest(const std::string& key, uint32_t& file_id) const;

    // Check if a key exists (has any file_ids)
    bool contains(const std::string& key) const;

    // Remove all file_ids for a key (used during GC compaction)
    void remove(const std::string& key);

    // Remove a specific file_id from a key's list
    void removeFile(const std::string& key, uint32_t file_id);

    // Iterate over all file_id lists
    void forEach(const std::function<void(const std::vector<uint32_t>&)>& fn) const;

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

    // Snapshot file format (v3):
    // [magic: 4 bytes]["L1IX"]
    // [version: 4 bytes][3]
    // [num_records: 8 bytes]
    // For each record:
    //   [hash: 8 bytes]
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
