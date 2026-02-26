#ifndef KVLITE_INTERNAL_READ_WRITE_DELTA_HASH_TABLE_H
#define KVLITE_INTERNAL_READ_WRITE_DELTA_HASH_TABLE_H

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "internal/delta_hash_table.h"
#include "internal/spinlock.h"

namespace kvlite {
namespace internal {

// Read-write delta hash table: always-mutable, thread-safe via per-bucket spinlocks.
//
// Stores (packed_version, id) pairs per key fingerprint.
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

    void addEntry(std::string_view key, uint64_t packed_version, uint32_t id);
    void addEntryByHash(uint64_t hash, uint64_t packed_version, uint32_t id);

    // Like addEntry, but returns true if the key's fingerprint group is new.
    bool addEntryIsNew(std::string_view key, uint64_t packed_version, uint32_t id);

    // Thread-safe collision-aware insertion.
    // Returns true if the key is new (fingerprint group newly created).
    bool addEntryChecked(std::string_view key, uint64_t packed_version, uint32_t id,
                         const DeltaHashTable::KeyResolver& resolver);

    // Remove entry (packed_version, id) for key. Returns true if fp group is now empty.
    bool removeEntry(std::string_view key, uint64_t packed_version, uint32_t id);

    // Update id for entry (packed_version, old_id) for key. Returns true if found.
    bool updateEntryId(std::string_view key, uint64_t packed_version,
                       uint32_t old_id, uint32_t new_id);

    // Locked read methods (hide base class unlocked versions)
    bool findAll(std::string_view key,
                 std::vector<uint64_t>& packed_versions,
                 std::vector<uint32_t>& ids) const;

    bool findFirst(std::string_view key,
                   uint64_t& packed_version, uint32_t& id) const;

    bool contains(std::string_view key) const;

    size_t size() const;
    size_t memoryUsage() const;
    void clear();

private:
    bool addImpl(uint32_t bi, uint32_t li, uint64_t fp,
                 uint64_t packed_version, uint32_t id);

    std::unique_ptr<Spinlock[]> bucket_locks_;
    BucketArena ext_arena_owned_;
    std::atomic<size_t> size_{0};
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_READ_WRITE_DELTA_HASH_TABLE_H
