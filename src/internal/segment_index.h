#ifndef KVLITE_INTERNAL_SEGMENT_INDEX_H
#define KVLITE_INTERNAL_SEGMENT_INDEX_H

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "kvlite/status.h"
#include "read_only_delta_hash_table.h"

namespace kvlite {
namespace internal {

class LogFile;

// SegmentIndex: In-memory per-file index mapping keys to (offset, packed_version) lists.
//
// Structure: key → [(offset₁, packed_version₁), (offset₂, packed_version₂), ...]
//            sorted by packed_version desc (latest/highest first)
//
// Packed versions encode (logical_version << 1) | tombstone_bit.
//
// All entries pertain to a single file. The index is append-only:
// no remove or update operations are supported.
// Write-once: no concurrency control or persistence needed.
class SegmentIndex {
public:
    SegmentIndex();
    ~SegmentIndex();

    SegmentIndex(const SegmentIndex&) = delete;
    SegmentIndex& operator=(const SegmentIndex&) = delete;
    SegmentIndex(SegmentIndex&&) noexcept;
    SegmentIndex& operator=(SegmentIndex&&) noexcept;

    // Append (offset, packed_version) to hash's list.
    void put(uint64_t hash, uint32_t offset, uint64_t packed_version);

    // Streaming batch build: feed entries in hash order, no decode overhead.
    void addBatchEntry(uint64_t hash, uint64_t packed_version, uint32_t offset);
    void endBatch();

    // Get all (offset, packed_version) pairs for a hash. Returns false if not found.
    // Pairs are ordered latest-first (highest packed_version first).
    bool get(uint64_t hash,
             std::vector<uint32_t>& offsets,
             std::vector<uint64_t>& packed_versions) const;

    // Get the latest entry for a hash with packed_version <= upper_bound.
    // Returns false if no matching entry exists.
    bool get(uint64_t hash, uint64_t upper_bound,
             uint64_t& offset, uint64_t& packed_version) const;

    // Get the latest (highest packed_version) entry for a hash.
    // Returns false if not found.
    bool getLatest(uint64_t hash,
                   uint32_t& offset, uint64_t& packed_version) const;

    // Check if a hash exists.
    bool contains(uint64_t hash) const;

    // Serialize to a LogFile.
    Status writeTo(LogFile& file);

    // Deserialize from a LogFile starting at the given byte offset.
    Status readFrom(const LogFile& file, uint64_t offset = 0);

    // Iterate over all entries, calling fn(offset, packed_version) for each.
    void forEach(const std::function<void(uint32_t offset, uint64_t packed_version)>& fn) const;

    // Iterate over all groups (hash-sorted). Callback receives:
    //   hash: the FNV-1a hash
    //   offsets: sorted desc (latest first)
    //   packed_versions: parallel array
    void forEachGroup(
        const std::function<void(uint64_t hash,
                                 const std::vector<uint32_t>& offsets,
                                 const std::vector<uint64_t>& packed_versions)>& fn) const;

    size_t keyCount() const;
    size_t entryCount() const;
    size_t memoryUsage() const;
    void clear();

    // Codec instrumentation
    uint64_t encodeCount() const { return dht_.encodeCount(); }
    uint64_t encodeTotalNs() const { return dht_.encodeTotalNs(); }
    uint64_t decodeCount() const { return dht_.decodeCount(); }
    uint64_t decodeTotalNs() const { return dht_.decodeTotalNs(); }

private:
    ReadOnlyDeltaHashTable dht_;
    size_t key_count_ = 0;
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_SEGMENT_INDEX_H
