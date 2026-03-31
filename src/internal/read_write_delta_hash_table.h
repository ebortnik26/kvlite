#ifndef KVLITE_INTERNAL_READ_WRITE_DELTA_HASH_TABLE_H
#define KVLITE_INTERNAL_READ_WRITE_DELTA_HASH_TABLE_H

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <vector>

#include "internal/bitmap.h"
#include "internal/delta_hash_table.h"
#include "internal/log_entry.h"
#include "internal/spinlock.h"

namespace kvlite {
namespace internal {

// Read-write delta hash table: always-mutable, thread-safe via per-bucket spinlocks.
//
// Stores (packed_version, id) pairs per key suffix.
// Thread-safe: per-bucket spinlocks protect concurrent access.
class ReadWriteDeltaHashTable : public DeltaHashTable {
public:
    using DeltaHashTable::Config;

    ReadWriteDeltaHashTable();
    explicit ReadWriteDeltaHashTable(const Config& config);
    ~ReadWriteDeltaHashTable();

    // Non-copyable, non-movable (contains mutex and atomics)
    ReadWriteDeltaHashTable(const ReadWriteDeltaHashTable&) = delete;
    ReadWriteDeltaHashTable& operator=(const ReadWriteDeltaHashTable&) = delete;
    ReadWriteDeltaHashTable(ReadWriteDeltaHashTable&&) = delete;
    ReadWriteDeltaHashTable& operator=(ReadWriteDeltaHashTable&&) = delete;

    void addEntry(uint64_t hash, uint64_t packed_version, uint32_t id);

    // Like addEntry, but returns true if the key's suffix group is new.
    bool addEntryIsNew(uint64_t hash, uint64_t packed_version, uint32_t id);

    // Batch-add entries that are sorted by hash. Holds each bucket's spinlock
    // for all entries in that bucket before moving to the next. Returns the
    // number of entries whose suffix group was new (for key_count tracking).
    // Batch-add entries sharing the same id (segment_id). Entries must be
    // hash-sorted. Holds each bucket's spinlock for all entries in that
    // bucket. Returns the number of new suffix groups (for key_count).
    size_t addEntriesBatch(const HashVersionPair* entries, size_t count, uint32_t id);

    // Remove entry (packed_version, id) for hash. Returns true if suffix group is now empty.
    bool removeEntry(uint64_t hash, uint64_t packed_version, uint32_t id);

    // Update id for entry (packed_version, old_id) for hash. Returns true if found.
    bool updateEntryId(uint64_t hash, uint64_t packed_version,
                       uint32_t old_id, uint32_t new_id);

    // Locked read methods (hide base class unlocked versions)
    bool findAll(uint64_t hash,
                 std::vector<uint64_t>& packed_versions,
                 std::vector<uint32_t>& ids) const;

    bool findFirst(uint64_t hash,
                   uint64_t& packed_version, uint32_t& id) const;

    bool findFirstBounded(uint64_t hash, uint64_t upper_bound,
                          uint64_t& packed_version, uint32_t& id) const;

    bool contains(uint64_t hash) const;

    // Prune stale versions from all flagged buckets. For each key with
    // multiple versions, keeps only the versions visible at the given
    // snapshot observation points. Returns the number of entries removed.
    size_t pruneStaleVersions(const std::vector<uint64_t>& snapshot_versions);

    // Average bucket utilization (used bits / total capacity bits), 0.0–1.0.
    double bucketUtilization() const;

    size_t size() const;
    size_t memoryUsage() const;
    void clear();

    // --- Binary snapshot support ---

    uint32_t extCount() const;
    const uint8_t* extSlotData(uint32_t one_based) const;
    void loadExtensions(const uint8_t* data, uint32_t count, uint32_t data_stride);
    void setSize(size_t n);

    // Snapshot bucket bi's chain (main + extensions) under its spinlock.
    // Returns chain length (0 = empty bucket). buf receives chain_len × stride bytes.
    uint32_t snapshotBucketChain(uint32_t bi, std::vector<uint8_t>& buf) const;

    // Load chain data into bucket bi + allocate extensions and wire ext_ptrs.
    void loadBucketChain(uint32_t bi, const uint8_t* data, uint8_t chain_len);

private:
    bool addImpl(uint32_t bi, uint64_t suffix,
                 uint64_t packed_version, uint32_t id);

    // Pruning helpers.
    std::vector<KeyEntry> gatherBucketKeys(uint32_t bi) const;
    void rewriteBucket(uint32_t bi, const std::vector<KeyEntry>& keys);

    std::unique_ptr<Spinlock[]> bucket_locks_;
    Bitmap multi_version_;  // 1 bit per bucket: set when a key gains 2nd version
    BucketArena ext_arena_owned_;
    std::atomic<size_t> size_{0};
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_READ_WRITE_DELTA_HASH_TABLE_H
