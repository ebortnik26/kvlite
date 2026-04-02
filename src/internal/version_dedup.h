#ifndef KVLITE_INTERNAL_VERSION_DEDUP_H
#define KVLITE_INTERNAL_VERSION_DEDUP_H

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace kvlite {
namespace internal {

// Deduplicate a version-sorted group of entries, keeping only those
// visible at the given observation points (snapshot versions +
// latestVersion).
//
// Input:
//   versions    — per-entry logical versions, sorted ascending.
//   snapshots   — observation points, sorted ascending. Must include
//                 latestVersion() as the last element.
//
// Output:
//   keep        — boolean per entry. true = keep, false = eliminate.
//
// For each snapshot, the latest entry whose version <= snapshot is kept.
// All other entries are eliminated.
//
// Complexity: O(N + S) where N = versions.size(), S = snapshots.size().
inline void dedupVersionGroup(
    const uint64_t* versions, size_t count,
    const std::vector<uint64_t>& snapshots,
    bool* keep) {

    // Default: eliminate all.
    for (size_t i = 0; i < count; ++i) {
        keep[i] = false;
    }

    if (snapshots.empty() || count == 0) return;

    // keeper[si] = index of the latest entry <= snapshots[si].
    // Two-pointer scan: walk entries left-to-right (version asc).
    size_t si = 0;
    size_t num_snaps = snapshots.size();
    std::vector<size_t> keeper(num_snaps, SIZE_MAX);

    for (size_t i = 0; i < count; ++i) {
        uint64_t ver = versions[i];
        // Advance past snapshots that this entry exceeds.
        while (si < num_snaps && ver > snapshots[si]) {
            ++si;
        }
        // Mark this entry as the best candidate for all snapshots >= ver.
        for (size_t s = si; s < num_snaps && ver <= snapshots[s]; ++s) {
            keeper[s] = i;
        }
    }

    for (size_t s = 0; s < num_snaps; ++s) {
        if (keeper[s] != SIZE_MAX) {
            keep[keeper[s]] = true;
        }
    }
}

// Prune stale versions from a KeyEntry whose packed_versions are
// sorted descending (newest first).  Keeps only versions visible at
// the given snapshot observation points.
//
// Single-pass sweep: walk pvs left→right (desc), snapshots right→left
// (desc).  For each snapshot the first pv whose logical version ≤
// snapshot is kept; all others are dropped.  pvs[0] is always kept
// (serves latestVersion, which is snapshots.back()).
//
// Modifies key.packed_versions and key.ids in-place.
// Returns the number of entries removed.
//
// Requires: snapshots sorted ascending, non-empty (must include
// latestVersion as the last element).
inline size_t pruneKeyVersionsDesc(
    std::vector<uint64_t>& packed_versions,
    std::vector<uint32_t>& ids,
    const std::vector<uint64_t>& snapshots) {

    size_t n = packed_versions.size();
    if (n <= 1 || snapshots.empty()) return 0;

    // Walk pvs left→right (descending) and snapshots right→left
    // (descending).  For each pv, consume all snapshots whose
    // observation point ≥ this version's logical value — this pv is
    // the latest version visible at those snapshots.  Keep the pv if
    // it serves at least one snapshot.
    int si = static_cast<int>(snapshots.size()) - 1;
    size_t write = 0;

    for (size_t i = 0; i < n; ++i) {
        uint64_t logical_ver = packed_versions[i] >> 1;  // PackedVersion shift
        // Does this version serve any remaining snapshot?
        // A snapshot S is served if logical_ver <= S.
        if (si >= 0 && logical_ver <= static_cast<uint64_t>(snapshots[si])) {
            packed_versions[write] = packed_versions[i];
            ids[write] = ids[i];
            ++write;
            // Consume all snapshots this version serves.
            while (si >= 0 && logical_ver <= static_cast<uint64_t>(snapshots[si])) {
                --si;
            }
        }
        // else: no remaining snapshot needs this version → drop
    }

    size_t removed = n - write;
    packed_versions.resize(write);
    ids.resize(write);
    return removed;
}

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_VERSION_DEDUP_H
