#include "internal/gc.h"

#include <queue>
#include <vector>

#include "internal/global_index.h"
#include "internal/log_file.h"
#include "internal/segment_index.h"
#include "internal/visibility_filter.h"

namespace kvlite {
namespace internal {

// Min-heap comparator: smaller hash first, then higher version first.
struct IterGreater {
    bool operator()(VisibleVersionIterator* a, VisibleVersionIterator* b) const {
        if (a->entry().hash != b->entry().hash)
            return a->entry().hash > b->entry().hash;
        return a->entry().log_entry.version() < b->entry().log_entry.version();
    }
};

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

    // 1. Create VisibleVersionIterator for each input.
    std::vector<VisibleVersionIterator> iterators;
    iterators.reserve(inputs.size());
    for (const auto& input : inputs) {
        iterators.push_back(VisibilityFilter::getVisibleVersions(
            global_index, input.index, input.segment_id,
            snapshot_versions, input.log_file, input.data_size));
    }

    // 2. Push all valid iterators into min-heap.
    std::priority_queue<VisibleVersionIterator*, std::vector<VisibleVersionIterator*>,
                        IterGreater> heap;
    for (auto& iter : iterators) {
        if (iter.valid()) {
            heap.push(&iter);
        }
    }

    // 3. If heap empty, return OK with empty result.
    if (heap.empty()) {
        return Status::OK();
    }

    // 4. Allocate first output segment.
    Segment output;
    uint32_t output_id = id_fn();
    Status s = output.create(path_fn(output_id), output_id);
    if (!s.ok()) return s;

    // 5. Drain heap.
    while (!heap.empty()) {
        VisibleVersionIterator* top = heap.top();
        heap.pop();

        const auto& entry = top->entry();
        const auto& le = entry.log_entry;

        // Check if we need to split to a new output segment.
        if (output.dataSize() > 0 &&
            output.dataSize() + le.serializedSize() > max_segment_size) {
            s = output.seal();
            if (!s.ok()) return s;
            result.outputs.push_back({output_id, std::move(output)});

            output = Segment();
            output_id = id_fn();
            s = output.create(path_fn(output_id), output_id);
            if (!s.ok()) return s;
        }

        // Write entry to current output.
        s = output.put(le.key, le.version(), le.value, le.tombstone());
        if (!s.ok()) return s;
        result.entries_written++;

        // Advance iterator; if still valid, push back.
        s = top->next();
        if (!s.ok()) return s;
        if (top->valid()) {
            heap.push(top);
        }
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
