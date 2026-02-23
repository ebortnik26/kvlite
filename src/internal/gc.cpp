#include "internal/gc.h"

#include <memory>
#include <vector>

#include "internal/entry_stream.h"
#include "internal/global_index.h"
#include "internal/log_entry.h"
#include "internal/log_file.h"
#include "internal/segment_index.h"

namespace kvlite {
namespace internal {

Status GC::merge(
    const GlobalIndex& global_index,
    const std::vector<uint64_t>& snapshot_versions,
    const std::vector<InputSegment>& inputs,
    uint64_t max_segment_size,
    const std::function<std::string(uint32_t)>& path_fn,
    const std::function<uint32_t()>& id_fn,
    Result& result) {

    result.outputs.clear();
    result.relocations.clear();
    result.eliminations.clear();
    result.entries_written = 0;
    result.entries_eliminated = 0;

    // Ext layout: [TagSourceExt | ClassifyExt]
    constexpr size_t kTagBase      = 0;
    constexpr size_t kClassifyBase = kTagBase + TagSourceExt::kSize;

    // 1. Build unfiltered tagSource streams and compute the union visible set.
    std::vector<std::unique_ptr<EntryStream>> streams;
    streams.reserve(inputs.size());
    std::unordered_map<uint64_t, std::set<uint32_t>> union_visible;

    for (const auto& input : inputs) {
        streams.push_back(stream::tagSource(
            stream::scan(input.log_file, input.data_size),
            input.segment_id, kTagBase));

        auto vs = computeVisibleSet(
            global_index, input.index, input.segment_id, snapshot_versions);
        for (auto& [hash, versions] : vs) {
            auto& dst = union_visible[hash];
            dst.insert(versions.begin(), versions.end());
        }
    }

    // 2. Merge all streams, then classify.
    auto pipeline = stream::classify(
        stream::merge(std::move(streams)),
        std::move(union_visible), kClassifyBase);

    // 3. If empty, return OK.
    if (!pipeline->valid()) {
        return Status::OK();
    }

    // 4. Allocate first output segment.
    Segment output;
    uint32_t output_id = id_fn();
    Status s = output.create(path_fn(output_id), output_id);
    if (!s.ok()) return s;

    // 5. Drain pipeline — read action and segment_id from ext slots.
    while (pipeline->valid()) {
        const auto& entry = pipeline->entry();
        auto action = static_cast<EntryAction>(
            entry.ext[kClassifyBase + ClassifyExt::kAction]);
        auto old_seg_id = static_cast<uint32_t>(
            entry.ext[kTagBase + TagSourceExt::kSegmentId]);

        if (action == EntryAction::kEliminate) {
            result.eliminations.push_back({
                std::string(entry.key), entry.version, old_seg_id});
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
            s = output.seal();
            if (!s.ok()) return s;
            result.outputs.push_back({output_id, std::move(output)});

            output = Segment();
            output_id = id_fn();
            s = output.create(path_fn(output_id), output_id);
            if (!s.ok()) return s;
        }

        s = output.put(entry.key, entry.version, entry.value, entry.tombstone);
        if (!s.ok()) return s;

        result.relocations.push_back({
            std::string(entry.key), entry.version, old_seg_id, output_id});
        result.entries_written++;

        s = pipeline->next();
        if (!s.ok()) return s;
    }

    // 6. Seal final output if it has entries.
    if (output.dataSize() > 0) {
        s = output.seal();
        if (!s.ok()) return s;
        result.outputs.push_back({output_id, std::move(output)});
    } else {
        output.close();
    }

    return Status::OK();
}

}  // namespace internal
}  // namespace kvlite
