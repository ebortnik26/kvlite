#ifndef KVLITE_INTERNAL_GC_STREAM_H
#define KVLITE_INTERNAL_GC_STREAM_H

#include <cstdint>
#include <memory>
#include <vector>

#include "internal/entry_stream.h"

namespace kvlite {
namespace internal {

// ---------------------------------------------------------------------------
// GC operator extension schemas.
// ---------------------------------------------------------------------------

struct GCTagSourceExt {
    enum Field : size_t { kSegmentId = 0 };
    static constexpr size_t kSize = 1;
};

struct GCDedupExt {
    enum Field : size_t { kAction = 0 };
    static constexpr size_t kSize = 1;
};

enum class EntryAction : uint64_t { kKeep = 0, kEliminate = 1 };

namespace stream {

// K-way merge over N EntryStreams in (hash asc, version asc) order.
// Used exclusively by GC to merge tagged segment streams.
std::unique_ptr<EntryStream> gcMerge(std::vector<std::unique_ptr<EntryStream>> inputs);

// Tag each entry with a source segment_id.
// Writes to ext[base + GCTagSourceExt::kSegmentId].
std::unique_ptr<EntryStream> gcTagSource(
    std::unique_ptr<EntryStream> input, uint32_t segment_id, size_t base);

// Dedup: mark each entry as kKeep or kEliminate based on snapshot visibility.
// Entries must arrive in (hash asc, version asc) order. For each hash group,
// the latest version <= each snapshot is kept; all others are eliminated.
// Writes EntryAction to ext[base + GCDedupExt::kAction].
std::unique_ptr<EntryStream> gcDedup(
    std::unique_ptr<EntryStream> input,
    const std::vector<uint64_t>& snapshot_versions,
    size_t base);

}  // namespace stream

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_GC_STREAM_H
