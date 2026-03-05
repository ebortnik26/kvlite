#ifndef KVLITE_INTERNAL_READ_ONLY_DELTA_HASH_TABLE_H
#define KVLITE_INTERNAL_READ_ONLY_DELTA_HASH_TABLE_H

#include <cstdint>

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

    void addEntry(uint64_t hash, uint64_t packed_version, uint32_t id);

    // Like addEntry, but returns true if the key's fingerprint group is new.
    bool addEntryIsNew(uint64_t hash, uint64_t packed_version, uint32_t id);

    // --- Streaming batch build (single-threaded, entries in hash order) ---

    // Feed one entry. Encodes the previous bucket when a boundary is crossed.
    void addBatchEntry(uint64_t hash, uint64_t packed_version, uint32_t id);

    // Encode the last pending bucket. Returns total distinct-key count.
    size_t endBatch();

    // Transition to read-only state. After this, writes are forbidden.
    void seal();

    // --- Stats ---

    size_t size() const;
    void clear();

private:
    void flushPendingBucket();

    BucketArena ext_arena_owned_;
    bool sealed_ = false;
    size_t size_ = 0;

    // Streaming batch state
    uint32_t pending_bi_ = UINT32_MAX;
    BucketContents pending_contents_;
    size_t pending_keys_ = 0;
    size_t pending_entries_ = 0;
    size_t batch_key_count_ = 0;
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_READ_ONLY_DELTA_HASH_TABLE_H
