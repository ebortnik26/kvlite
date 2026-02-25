#ifndef KVLITE_INTERNAL_L2_INDEX_H
#define KVLITE_INTERNAL_L2_INDEX_H

#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
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

    // Append (offset, packed_version) to key's list.
    void put(std::string_view key, uint32_t offset, uint64_t packed_version);

    // Get all (offset, packed_version) pairs for a key. Returns false if key doesn't exist.
    // Pairs are ordered latest-first (highest packed_version first).
    bool get(const std::string& key,
             std::vector<uint32_t>& offsets,
             std::vector<uint64_t>& packed_versions) const;

    // Get the latest entry for a key with packed_version <= upper_bound.
    // Returns false if no matching entry exists.
    bool get(const std::string& key, uint64_t upper_bound,
             uint64_t& offset, uint64_t& packed_version) const;

    // Get the latest (highest packed_version) entry for a key.
    // Returns false if key doesn't exist.
    bool getLatest(const std::string& key,
                   uint32_t& offset, uint64_t& packed_version) const;

    // Check if a key exists.
    bool contains(const std::string& key) const;

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

private:
    ReadOnlyDeltaHashTable dht_;
    size_t key_count_ = 0;
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_L2_INDEX_H
