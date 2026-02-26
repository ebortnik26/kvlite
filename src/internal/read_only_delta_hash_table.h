#ifndef KVLITE_INTERNAL_READ_ONLY_DELTA_HASH_TABLE_H
#define KVLITE_INTERNAL_READ_ONLY_DELTA_HASH_TABLE_H

#include <cstdint>
#include <string_view>

#include "internal/delta_hash_table.h"

namespace kvlite {
namespace internal {

// Read-only delta hash table: write-once lifecycle with no locking.
//
// Building phase: addEntry/addEntryByHash append entries (single-threaded).
// After seal(), the table becomes immutable — writes are forbidden.
// Reads work in both phases (safe: single-threaded during building,
// immutable after seal).
class ReadOnlyDeltaHashTable : public DeltaHashTable {
public:
    using DeltaHashTable::Config;

    ReadOnlyDeltaHashTable();
    explicit ReadOnlyDeltaHashTable(const Config& config);
    ~ReadOnlyDeltaHashTable();

    ReadOnlyDeltaHashTable(const ReadOnlyDeltaHashTable&) = delete;
    ReadOnlyDeltaHashTable& operator=(const ReadOnlyDeltaHashTable&) = delete;
    ReadOnlyDeltaHashTable(ReadOnlyDeltaHashTable&&) noexcept;
    ReadOnlyDeltaHashTable& operator=(ReadOnlyDeltaHashTable&&) noexcept;

    // --- Building phase (single-threaded, before seal) ---

    void addEntry(std::string_view key, uint64_t packed_version, uint32_t id);
    void addEntryByHash(uint64_t hash, uint64_t packed_version, uint32_t id);

    // Like addEntry, but returns true if the key's fingerprint group is new.
    bool addEntryIsNew(std::string_view key, uint64_t packed_version, uint32_t id);

    // Transition to read-only state. After this, writes are forbidden.
    void seal();

    // --- Stats ---

    size_t size() const;
    void clear();

private:
    BucketArena ext_arena_owned_;
    bool sealed_ = false;
    size_t size_ = 0;
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_READ_ONLY_DELTA_HASH_TABLE_H
