#include "internal/gc.h"

#include <memory>
#include <vector>

#include "internal/entry_stream.h"
#include "internal/gc_stream.h"
#include "internal/log_entry.h"

namespace kvlite {
namespace internal {

// ---------------------------------------------------------------------------
// GC::merge — compaction entry point
// ---------------------------------------------------------------------------

Status GC::merge(
    const std::vector<uint64_t>& snapshot_versions,
    const std::vector<const Segment*>& inputs,
    uint64_t max_segment_size,
    const std::function<std::string(uint32_t)>& path_fn,
    const std::function<uint32_t()>& id_fn,
    const RelocateFn& on_relocate,
    const EliminateFn& on_eliminate,
    Result& result,
    bool buffered_writes,
    uint16_t num_partitions,
    FlushPool* flush_pool) {

    result.outputs.clear();
    result.entries_written = 0;
    result.entries_eliminated = 0;

    // Ext layout: [GCTagSourceExt | GCDedupExt]
    constexpr size_t kTagBase      = 0;
    constexpr size_t kDedupBase = kTagBase + GCTagSourceExt::kSize;

    // 1. Build tagSource streams — one per partition per input segment.
    std::vector<std::unique_ptr<EntryStream>> streams;

    for (const auto* input : inputs) {
        for (uint16_t p = 0; p < input->numPartitions(); ++p) {
            streams.push_back(stream::gcTagSource(
                stream::scan(input->logFile(p), input->dataSize(p)),
                input->getId(), kTagBase));
        }
    }

    // 2. Merge all streams, then dedup using snapshot versions.
    auto pipeline = stream::gcDedup(
        stream::gcMerge(std::move(streams)),
        snapshot_versions, kDedupBase);

    // 3. If empty, return OK.
    if (!pipeline->valid()) {
        return Status::OK();
    }

    // 4. Allocate first output segment.
    Segment output;
    uint32_t output_id = id_fn();
    Status s = output.create(path_fn(output_id), output_id,
                              num_partitions, buffered_writes);
    if (!s.ok()) return s;
    output.setLineageType(LineageType::kGC);

    // 5. Drain pipeline — read action and segment_id from ext slots.
    while (pipeline->valid()) {
        const auto& entry = pipeline->entry();
        auto action = static_cast<EntryAction>(
            entry.ext[kDedupBase + GCDedupExt::kAction]);
        auto old_seg_id = static_cast<uint32_t>(
            entry.ext[kTagBase + GCTagSourceExt::kSegmentId]);

        if (action == EntryAction::kEliminate) {
            on_eliminate(entry.hash, entry.pv.data, old_seg_id);
            output.addLineageDeleted(entry.hash, entry.pv.data, old_seg_id);
            result.entries_eliminated++;
            s = pipeline->next();
            if (!s.ok()) return s;
            continue;
        }

        // kKeep: write to output segment.
        size_t serialized_size = LogEntry::kHeaderSize + entry.key.size() +
                                 entry.value.size() + LogEntry::kChecksumSize;
        if (output.dataSize() > 0 &&
            output.dataSize() + serialized_size > max_segment_size) {
            s = output.seal(flush_pool);
            if (!s.ok()) return s;
            result.outputs.push_back(std::move(output));

            output = Segment();
            output_id = id_fn();
            s = output.create(path_fn(output_id), output_id,
                               num_partitions, buffered_writes);
            if (!s.ok()) return s;
            output.setLineageType(LineageType::kGC);
        }

        uint64_t entry_offset;
        s = output.appendEntry(entry.key, entry.version(), entry.value,
                               entry.tombstone(), entry.hash, entry_offset);
        if (!s.ok()) return s;

        output.addLineagePresent(entry.hash, entry.pv.data, old_seg_id);
        on_relocate(entry.hash, entry.pv.data, old_seg_id, output_id);
        result.entries_written++;

        s = pipeline->next();
        if (!s.ok()) return s;
    }

    // 6. Seal final output if it has entries.
    if (output.dataSize() > 0) {
        s = output.seal();
        if (!s.ok()) return s;
        result.outputs.push_back(std::move(output));
    } else {
        output.close();
    }

    return Status::OK();
}

}  // namespace internal
}  // namespace kvlite
