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
    result.entries_written = 0;

    // 1. Create scanVisible stream for each input.
    std::vector<std::unique_ptr<EntryStream>> streams;
    streams.reserve(inputs.size());
    for (const auto& input : inputs) {
        streams.push_back(stream::scanVisible(
            global_index, input.index, input.segment_id,
            snapshot_versions, input.log_file, input.data_size));
    }

    // 2. Create merged stream.
    auto merged = stream::merge(std::move(streams));

    // 3. If empty, return OK.
    if (!merged->valid()) {
        return Status::OK();
    }

    // 4. Allocate first output segment.
    Segment output;
    uint32_t output_id = id_fn();
    Status s = output.create(path_fn(output_id), output_id);
    if (!s.ok()) return s;

    // 5. Drain stream.
    while (merged->valid()) {
        const auto& entry = merged->entry();

        // Check if we need to split to a new output segment.
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

        // Write entry to current output.
        s = output.put(entry.key, entry.version, entry.value, entry.tombstone);
        if (!s.ok()) return s;
        result.entries_written++;

        // Advance stream.
        s = merged->next();
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
