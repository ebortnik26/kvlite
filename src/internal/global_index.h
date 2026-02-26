#ifndef KVLITE_INTERNAL_L1_INDEX_H
#define KVLITE_INTERNAL_L1_INDEX_H

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "internal/read_write_delta_hash_table.h"
#include "kvlite/status.h"

namespace kvlite {
namespace internal {

class GlobalIndexWAL;
class Manifest;

// GlobalIndex: In-memory index mapping keys to (segment_id, packed_version) lists,
// with built-in concurrency control and persistence.
//
// Structure: key -> [(segment_id, packed_version), ...]
//            sorted by packed_version desc (latest/highest first)
//
// Packed versions encode (logical_version << 1) | tombstone_bit.
//
// Backed by ReadWriteDeltaHashTable which stores (packed_version, id=segment_id) pairs.
//
// The GlobalIndex is always fully loaded in memory. It is persisted via:
// 1. WAL (append-only delta log for crash recovery)
// 2. Periodic snapshots (full dump every N updates + on shutdown)
//
// Thread-safety: Index operations are thread-safe via per-bucket spinlocks
// in the underlying DeltaHashTable. Lifecycle methods (open/recover/close)
// must be called from a single thread.
class GlobalIndex {
public:
    struct Options {
        // Number of updates before auto-snapshot (0 = disabled)
        uint64_t snapshot_interval = 10'000'000;

        // Sync WAL to disk on every write (slower but more durable)
        bool sync_writes = false;
    };

    GlobalIndex();
    ~GlobalIndex();

    // Non-copyable
    GlobalIndex(const GlobalIndex&) = delete;
    GlobalIndex& operator=(const GlobalIndex&) = delete;

    // --- Lifecycle ---

    Status open(const std::string& db_path, Manifest& manifest,
                const Options& options);
    Status close();
    bool isOpen() const;

    // --- Index Operations ---

    using KeyResolver = DeltaHashTable::KeyResolver;

    Status put(const std::string& key, uint64_t packed_version, uint32_t segment_id);

    // Collision-aware put. Uses resolver to detect fingerprint collisions.
    Status putChecked(const std::string& key, uint64_t packed_version,
                      uint32_t segment_id, const KeyResolver& resolver);

    bool get(const std::string& key,
             std::vector<uint32_t>& segment_ids,
             std::vector<uint64_t>& packed_versions) const;

    bool get(const std::string& key, uint64_t upper_bound,
             uint64_t& packed_version, uint32_t& segment_id) const;

    Status getLatest(const std::string& key,
                     uint64_t& packed_version, uint32_t& segment_id) const;

    bool contains(const std::string& key) const;

    // Update segment_id for an existing entry.
    Status relocate(const std::string& key, uint64_t packed_version,
                    uint32_t old_segment_id, uint32_t new_segment_id);

    // Remove an entry. If the key's fingerprint group becomes empty, decrements key_count_.
    Status eliminate(const std::string& key, uint64_t packed_version,
                     uint32_t segment_id);

    // --- Iteration ---

    void forEachGroup(
        const std::function<void(uint64_t hash,
                                 const std::vector<uint64_t>& packed_versions,
                                 const std::vector<uint32_t>& segment_ids)>& fn) const;

    void clear();

    // --- WAL commit (hides producer IDs from callers) ---

    Status commitWB();
    Status commitGC();

    // --- Statistics ---

    size_t keyCount() const;
    size_t entryCount() const;
    size_t memoryUsage() const;
    uint64_t updatesSinceSnapshot() const;

    // --- Persistence (low-level) ---

    Status saveSnapshot(const std::string& path) const;
    Status loadSnapshot(const std::string& path);

    // Snapshot file format (v7, hash-based):
    // [magic: 4 bytes]["L1IX" (legacy)]
    // [format_version: 4 bytes][7]
    // [num_entries: 8 bytes]
    // [key_count: 8 bytes]
    // Per entry (via forEach):
    //   [hash: 8 bytes]
    //   [packed_version: 8 bytes]
    //   [segment_id: 4 bytes]
    // [checksum: 4 bytes]

private:
    // --- Core DHT mutations (no WAL, no snapshot counter) ---
    // Used by both the public API (which also writes to WAL) and WAL replay.
    void applyPut(std::string_view key, uint64_t packed_version, uint32_t segment_id);
    void applyRelocate(std::string_view key, uint64_t packed_version,
                       uint32_t old_segment_id, uint32_t new_segment_id);
    void applyEliminate(std::string_view key, uint64_t packed_version,
                        uint32_t segment_id);

    Status maybeSnapshot();
    std::string snapshotPath() const;

    // --- Data ---
    ReadWriteDeltaHashTable dht_;
    size_t key_count_ = 0;

    // --- Lifecycle / persistence ---
    std::string db_path_;
    Options options_;
    bool is_open_ = false;
    std::unique_ptr<GlobalIndexWAL> wal_;
    uint64_t updates_since_snapshot_ = 0;
};

} // namespace internal
} // namespace kvlite

#endif // KVLITE_INTERNAL_L1_INDEX_H
