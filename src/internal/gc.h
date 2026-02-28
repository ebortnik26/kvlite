#ifndef KVLITE_INTERNAL_GC_H
#define KVLITE_INTERNAL_GC_H

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "internal/log_entry.h"
#include "internal/segment.h"
#include "kvlite/status.h"

namespace kvlite {
namespace internal {

// GC: K-way merge compaction of N input segments into M output segments.
//
// Merges visible entries from N input segments using composable EntryStream
// operators (scan + tagSource + merge + classify). Output entries arrive in
// (hash asc, version asc) order. Output segments are split by a max size
// limit and are hash-partitioned by construction.
//
// GC::merge is a pure data transformation — the caller handles GlobalIndex
// bookkeeping (registering new segments, removing old ones) via callbacks.
class GC {
public:
    using RelocateFn = std::function<void(
        uint64_t hkey, uint64_t packed_version,
        uint32_t old_segment_id, uint32_t new_segment_id)>;
    using EliminateFn = std::function<void(
        uint64_t hkey, uint64_t packed_version,
        uint32_t old_segment_id)>;

    struct Relocation {
        uint64_t hkey;
        uint64_t packed_version;
        uint32_t old_segment_id;
        uint32_t new_segment_id;
    };

    struct Elimination {
        uint64_t hkey;
        uint64_t packed_version;
        uint32_t old_segment_id;
    };

    struct Result {
        std::vector<Segment> outputs;   // sealed output segments
        size_t entries_written = 0;
        size_t entries_eliminated = 0;
    };

    // Merge visible entries from input segments into new output segments.
    //
    // Parameters:
    //   snapshot_versions: active snapshot versions for visibility decisions.
    //   inputs: the N input segments (Readable state).
    //   max_segment_size: data region limit per output segment.
    //   path_fn(segment_id) → path: generates file path for a new segment.
    //   id_fn() → segment_id: allocates a fresh segment ID.
    //   on_relocate: called for each entry written to an output segment.
    //   on_eliminate: called for each entry dropped during compaction.
    //   result: populated with output segments and stats.
    static Status merge(
        const std::vector<uint64_t>& snapshot_versions,
        const std::vector<const Segment*>& inputs,
        uint64_t max_segment_size,
        const std::function<std::string(uint32_t)>& path_fn,
        const std::function<uint32_t()>& id_fn,
        const RelocateFn& on_relocate,
        const EliminateFn& on_eliminate,
        Result& result);
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_GC_H
