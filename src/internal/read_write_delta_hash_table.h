#ifndef KVLITE_INTERNAL_READ_WRITE_DELTA_HASH_TABLE_H
#define KVLITE_INTERNAL_READ_WRITE_DELTA_HASH_TABLE_H

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <vector>

#include "internal/delta_hash_table.h"
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

    bool contains(uint64_t hash) const;

    size_t size() const;
    size_t memoryUsage() const;
    void clear();

    // --- Binary snapshot support ---

    uint32_t extCount() const;
    const uint8_t* extSlotData(uint32_t one_based) const;
    void loadExtensions(const uint8_t* data, uint32_t count, uint32_t data_stride);
    void setSize(size_t n);

private:
    bool addImpl(uint32_t bi, uint64_t suffix,
                 uint64_t packed_version, uint32_t id);

    std::unique_ptr<Spinlock[]> bucket_locks_;
    BucketArena ext_arena_owned_;
    std::atomic<size_t> size_{0};
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_READ_WRITE_DELTA_HASH_TABLE_H
