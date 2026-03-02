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

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_VERSION_DEDUP_H
