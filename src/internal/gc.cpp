#include "internal/gc.h"

#include <algorithm>
#include <cstring>
#include <memory>
#include <queue>
#include <vector>

#include "internal/crc32.h"
#include "internal/delta_hash_table_base.h"
#include "internal/global_index.h"
#include "internal/log_file.h"
#include "internal/segment_index.h"

namespace kvlite {
namespace internal {

// ---------------------------------------------------------------------------
// computeVisibleSet: merge-join GlobalIndex and SegmentIndex to find the set
// of (hash -> {versions}) visible in the given segment.
// ---------------------------------------------------------------------------

static std::unordered_map<uint64_t, std::set<uint32_t>> computeVisibleSet(
    const GlobalIndex& global_index,
    const SegmentIndex& segment_index,
    uint32_t segment_id,
    const std::vector<uint64_t>& snapshot_versions) {

    std::unordered_map<uint64_t, std::set<uint32_t>> result;

    if (snapshot_versions.empty()) {
        return result;
    }

    // Phase 1: Collect both sides.
    struct GlobalGroup {
        uint64_t hash;
        std::vector<uint32_t> versions;     // sorted desc
        std::vector<uint32_t> segment_ids;  // parallel array
    };

    std::vector<GlobalGroup> global_groups;
    global_index.forEachGroup(
        [&global_groups](uint64_t hash,
                         const std::vector<uint32_t>& versions,
                         const std::vector<uint32_t>& segment_ids) {
            global_groups.push_back({hash, versions, segment_ids});
        });

    std::vector<uint64_t> local_hashes;
    segment_index.forEachGroup(
        [&local_hashes](uint64_t hash,
                        const std::vector<uint32_t>&,
                        const std::vector<uint32_t>&) {
            local_hashes.push_back(hash);
        });

    // Sort both sides by hash.
    std::sort(global_groups.begin(), global_groups.end(),
              [](const GlobalGroup& a, const GlobalGroup& b) {
                  return a.hash < b.hash;
              });
    std::sort(local_hashes.begin(), local_hashes.end());

    // Phase 2: Merge-join on hash.
    size_t gi = 0;
    size_t li = 0;

    while (gi < global_groups.size() && li < local_hashes.size()) {
        uint64_t gh = global_groups[gi].hash;
        uint64_t lh = local_hashes[li];

        if (gh < lh) {
            ++gi;
        } else if (gh > lh) {
            ++li;
        } else {
            const auto& global_versions = global_groups[gi].versions;
            const auto& global_seg_ids = global_groups[gi].segment_ids;

            std::set<uint32_t> pinned;

            for (uint64_t snap : snapshot_versions) {
                for (size_t j = 0; j < global_versions.size(); ++j) {
                    if (static_cast<uint64_t>(global_versions[j]) <= snap) {
                        if (global_seg_ids[j] == segment_id) {
                            pinned.insert(global_versions[j]);
                        }
                        break;
                    }
                }
            }

            if (!pinned.empty()) {
                result[gh] = std::move(pinned);
            }
            ++gi;
            ++li;
        }
    }

    return result;
}

// ---------------------------------------------------------------------------
// VisibleVersionIterator
// ---------------------------------------------------------------------------

VisibleVersionIterator::VisibleVersionIterator(
    std::unordered_map<uint64_t, std::set<uint32_t>> visible_set,
    const LogFile& log_file, uint64_t data_size)
    : log_file_(log_file),
      data_size_(data_size),
      visible_set_(std::move(visible_set)) {
    // advance() sets valid_ if a visible entry is found.
    advance();
}

Status VisibleVersionIterator::readNextEntry(LogEntry& out) {
    // Read header (14 bytes).
    uint8_t hdr[LogEntry::kHeaderSize];
    Status s = log_file_.readAt(file_offset_, hdr, LogEntry::kHeaderSize);
    if (!s.ok()) return s;

    uint64_t version;
    uint16_t key_len_raw;
    uint32_t vl;
    std::memcpy(&version, hdr, 8);
    std::memcpy(&key_len_raw, hdr + 8, 2);
    std::memcpy(&vl, hdr + 10, 4);

    bool tombstone = (key_len_raw & LogEntry::kTombstoneBit) != 0;
    uint32_t kl = key_len_raw & ~LogEntry::kTombstoneBit;

    // Read full entry for CRC validation.
    size_t entry_size = LogEntry::kHeaderSize + kl + vl + LogEntry::kChecksumSize;
    uint8_t stack_buf[4096];
    std::unique_ptr<uint8_t[]> heap_buf;
    uint8_t* buf = stack_buf;
    if (entry_size > sizeof(stack_buf)) {
        heap_buf = std::make_unique<uint8_t[]>(entry_size);
        buf = heap_buf.get();
    }

    s = log_file_.readAt(file_offset_, buf, entry_size);
    if (!s.ok()) return s;

    // Validate CRC.
    size_t payload_len = LogEntry::kHeaderSize + kl + vl;
    uint32_t stored_crc;
    std::memcpy(&stored_crc, buf + payload_len, 4);
    uint32_t computed_crc = crc32(buf, payload_len);
    if (stored_crc != computed_crc) {
        return Status::Corruption("VisibleVersionIterator: CRC mismatch at offset " +
                                  std::to_string(file_offset_));
    }

    out.pv = PackedVersion(version, tombstone);
    out.key.assign(reinterpret_cast<const char*>(buf + LogEntry::kHeaderSize), kl);
    out.value.assign(reinterpret_cast<const char*>(buf + LogEntry::kHeaderSize + kl), vl);

    file_offset_ += entry_size;
    return Status::OK();
}

Status VisibleVersionIterator::advance() {
    while (file_offset_ < data_size_) {
        LogEntry entry;
        Status s = readNextEntry(entry);
        if (!s.ok()) {
            valid_ = false;
            return s;
        }

        uint64_t hash = dhtHashBytes(entry.key.data(), entry.key.size());
        auto it = visible_set_.find(hash);
        if (it != visible_set_.end()) {
            uint32_t v = static_cast<uint32_t>(entry.version());
            if (it->second.count(v)) {
                current_.hash = hash;
                current_.log_entry = std::move(entry);
                valid_ = true;
                return Status::OK();
            }
        }
    }
    valid_ = false;
    return Status::OK();
}

Status VisibleVersionIterator::next() {
    return advance();
}

// ---------------------------------------------------------------------------
// GC::getVisibleVersions
// ---------------------------------------------------------------------------

VisibleVersionIterator GC::getVisibleVersions(
    const GlobalIndex& global_index,
    const SegmentIndex& segment_index,
    uint32_t segment_id,
    const std::vector<uint64_t>& snapshot_versions,
    const LogFile& log_file,
    uint64_t data_size) {

    auto visible = computeVisibleSet(global_index, segment_index,
                                     segment_id, snapshot_versions);
    return VisibleVersionIterator(std::move(visible), log_file, data_size);
}

// ---------------------------------------------------------------------------
// GC::merge
// ---------------------------------------------------------------------------

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
        iterators.push_back(GC::getVisibleVersions(
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
