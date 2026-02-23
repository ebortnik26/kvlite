#include "internal/entry_stream.h"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <memory>
#include <string>

#include "internal/crc32.h"
#include "internal/delta_hash_table_base.h"
#include "internal/global_index.h"
#include "internal/log_entry.h"
#include "internal/log_file.h"
#include "internal/segment_index.h"
#include "internal/write_buffer.h"

namespace kvlite {
namespace internal {

// ---------------------------------------------------------------------------
// computeVisibleSet
// ---------------------------------------------------------------------------

std::unordered_map<uint64_t, std::set<uint32_t>> computeVisibleSet(
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
// computeLatestSet
// ---------------------------------------------------------------------------

std::unordered_map<uint64_t, uint32_t> computeLatestSet(
    const GlobalIndex& global_index,
    uint32_t segment_id,
    uint64_t snapshot_version) {

    std::unordered_map<uint64_t, uint32_t> result;

    global_index.forEachGroup(
        [&result, segment_id, snapshot_version](
            uint64_t hash,
            const std::vector<uint32_t>& versions,
            const std::vector<uint32_t>& segment_ids) {
            // versions are sorted desc. Find first <= snapshot_version.
            for (size_t i = 0; i < versions.size(); ++i) {
                if (static_cast<uint64_t>(versions[i]) <= snapshot_version) {
                    if (segment_ids[i] == segment_id) {
                        result[hash] = versions[i];
                    }
                    break;
                }
            }
        });

    return result;
}

// ---------------------------------------------------------------------------
// ScanStream — reads ALL entries from a LogFile
// ---------------------------------------------------------------------------

class ScanStream : public EntryStream {
public:
    ScanStream(const LogFile& log_file, uint64_t data_size)
        : log_file_(log_file), data_size_(data_size) {
        if (data_size_ > 0) {
            readNext();
        }
    }

    bool valid() const override { return valid_; }
    const Entry& entry() const override { return current_; }

    Status next() override {
        return readNext();
    }

private:
    Status readNext() {
        if (file_offset_ >= data_size_) {
            valid_ = false;
            return Status::OK();
        }

        // Read header (14 bytes).
        uint8_t hdr[LogEntry::kHeaderSize];
        Status s = log_file_.readAt(file_offset_, hdr, LogEntry::kHeaderSize);
        if (!s.ok()) { valid_ = false; return s; }

        uint64_t version;
        uint16_t key_len_raw;
        uint32_t vl;
        std::memcpy(&version, hdr, 8);
        std::memcpy(&key_len_raw, hdr + 8, 2);
        std::memcpy(&vl, hdr + 10, 4);

        bool tombstone = (key_len_raw & LogEntry::kTombstoneBit) != 0;
        uint32_t kl = key_len_raw & ~LogEntry::kTombstoneBit;

        // Read full entry for CRC validation into persistent buf_.
        size_t entry_size = LogEntry::kHeaderSize + kl + vl + LogEntry::kChecksumSize;
        buf_.resize(entry_size);

        s = log_file_.readAt(file_offset_, buf_.data(), entry_size);
        if (!s.ok()) { valid_ = false; return s; }

        // Validate CRC.
        size_t payload_len = LogEntry::kHeaderSize + kl + vl;
        uint32_t stored_crc;
        std::memcpy(&stored_crc, buf_.data() + payload_len, 4);
        uint32_t computed_crc = crc32(buf_.data(), payload_len);
        if (stored_crc != computed_crc) {
            valid_ = false;
            return Status::Corruption("ScanStream: CRC mismatch at offset " +
                                      std::to_string(file_offset_));
        }

        const char* key_ptr = reinterpret_cast<const char*>(buf_.data() + LogEntry::kHeaderSize);
        const char* val_ptr = key_ptr + kl;

        current_.hash = dhtHashBytes(key_ptr, kl);
        current_.key = std::string_view(key_ptr, kl);
        current_.value = std::string_view(val_ptr, vl);
        current_.version = version;
        current_.tombstone = tombstone;
        valid_ = true;

        file_offset_ += entry_size;
        return Status::OK();
    }

    const LogFile& log_file_;
    uint64_t data_size_;
    uint64_t file_offset_ = 0;
    std::vector<uint8_t> buf_;  // persistent buffer — string_views point into it
    Entry current_;
    bool valid_ = false;
};

// ---------------------------------------------------------------------------
// FilterStream — wraps any EntryStream + predicate
// ---------------------------------------------------------------------------

class FilterStream : public EntryStream {
public:
    FilterStream(std::unique_ptr<EntryStream> input, Predicate pred)
        : input_(std::move(input)), pred_(std::move(pred)) {
        advance();
    }

    bool valid() const override { return input_->valid(); }
    const Entry& entry() const override { return input_->entry(); }

    Status next() override {
        Status s = input_->next();
        if (!s.ok()) return s;
        return advance();
    }

private:
    Status advance() {
        while (input_->valid()) {
            if (pred_(input_->entry())) {
                return Status::OK();
            }
            Status s = input_->next();
            if (!s.ok()) return s;
        }
        return Status::OK();
    }

    std::unique_ptr<EntryStream> input_;
    Predicate pred_;
};

// ---------------------------------------------------------------------------
// MergeStream — K-way merge over N EntryStreams
// ---------------------------------------------------------------------------

class MergeStream : public EntryStream {
public:
    explicit MergeStream(std::vector<std::unique_ptr<EntryStream>> inputs)
        : inputs_(std::move(inputs)) {
        for (auto& s : inputs_) {
            if (s->valid()) {
                heap_.push(s.get());
            }
        }
    }

    bool valid() const override { return !heap_.empty(); }
    const Entry& entry() const override { return heap_.top()->entry(); }

    Status next() override {
        if (heap_.empty()) {
            return Status::NotFound("MergeStream exhausted");
        }

        EntryStream* top = heap_.top();
        heap_.pop();

        Status s = top->next();
        if (!s.ok()) return s;

        if (top->valid()) {
            heap_.push(top);
        }

        return Status::OK();
    }

private:
    struct StreamGreater {
        bool operator()(EntryStream* a, EntryStream* b) const {
            if (a->entry().hash != b->entry().hash)
                return a->entry().hash > b->entry().hash;
            return a->entry().version > b->entry().version;
        }
    };

    std::vector<std::unique_ptr<EntryStream>> inputs_;
    std::priority_queue<EntryStream*, std::vector<EntryStream*>, StreamGreater> heap_;
};

// ---------------------------------------------------------------------------
// TagSourceStream — writes segment_id to ext[base + TagSourceExt::kSegmentId]
// ---------------------------------------------------------------------------

class TagSourceStream : public EntryStream {
public:
    TagSourceStream(std::unique_ptr<EntryStream> input, uint32_t segment_id, size_t base)
        : input_(std::move(input)), segment_id_(segment_id), base_(base) {
        assert(base_ + TagSourceExt::kSize <= Entry::kMaxExt);
        if (input_->valid()) {
            stamp();
        }
    }

    bool valid() const override { return input_->valid(); }
    const Entry& entry() const override { return current_; }

    Status next() override {
        Status s = input_->next();
        if (!s.ok()) return s;
        if (input_->valid()) {
            stamp();
        }
        return Status::OK();
    }

private:
    void stamp() {
        current_ = input_->entry();
        current_.ext[base_ + TagSourceExt::kSegmentId] = segment_id_;
    }

    std::unique_ptr<EntryStream> input_;
    uint32_t segment_id_;
    size_t base_;
    Entry current_;
};

// ---------------------------------------------------------------------------
// ClassifyStream — writes EntryAction to ext[base + ClassifyExt::kAction]
// ---------------------------------------------------------------------------

class ClassifyStream : public EntryStream {
public:
    ClassifyStream(std::unique_ptr<EntryStream> input,
                   std::unordered_map<uint64_t, std::set<uint32_t>> visible_set,
                   size_t base)
        : input_(std::move(input)),
          visible_set_(std::move(visible_set)),
          base_(base) {
        assert(base_ + ClassifyExt::kSize <= Entry::kMaxExt);
        if (input_->valid()) {
            classify();
        }
    }

    bool valid() const override { return input_->valid(); }
    const Entry& entry() const override { return current_; }

    Status next() override {
        Status s = input_->next();
        if (!s.ok()) return s;
        if (input_->valid()) {
            classify();
        }
        return Status::OK();
    }

private:
    void classify() {
        current_ = input_->entry();
        const auto it = visible_set_.find(current_.hash);
        if (it != visible_set_.end()) {
            uint32_t v = static_cast<uint32_t>(current_.version);
            if (it->second.count(v) > 0) {
                current_.ext[base_ + ClassifyExt::kAction] =
                    static_cast<uint64_t>(EntryAction::kKeep);
                return;
            }
        }
        current_.ext[base_ + ClassifyExt::kAction] =
            static_cast<uint64_t>(EntryAction::kEliminate);
    }

    std::unique_ptr<EntryStream> input_;
    std::unordered_map<uint64_t, std::set<uint32_t>> visible_set_;
    size_t base_;
    Entry current_;
};

// ---------------------------------------------------------------------------
// Factory functions
// ---------------------------------------------------------------------------

namespace stream {

std::unique_ptr<EntryStream> scan(const LogFile& lf, uint64_t data_size) {
    return std::make_unique<ScanStream>(lf, data_size);
}

std::unique_ptr<EntryStream> filter(std::unique_ptr<EntryStream> input, Predicate pred) {
    return std::make_unique<FilterStream>(std::move(input), std::move(pred));
}

std::unique_ptr<EntryStream> merge(std::vector<std::unique_ptr<EntryStream>> inputs) {
    return std::make_unique<MergeStream>(std::move(inputs));
}

std::unique_ptr<EntryStream> tagSource(
    std::unique_ptr<EntryStream> input, uint32_t segment_id, size_t base) {
    return std::make_unique<TagSourceStream>(std::move(input), segment_id, base);
}

std::unique_ptr<EntryStream> classify(
    std::unique_ptr<EntryStream> input,
    std::unordered_map<uint64_t, std::set<uint32_t>> visible_set,
    size_t base) {
    return std::make_unique<ClassifyStream>(std::move(input), std::move(visible_set), base);
}

std::unique_ptr<EntryStream> scanVisible(
    const GlobalIndex& gi, const SegmentIndex& si,
    uint32_t segment_id, const std::vector<uint64_t>& snapshot_versions,
    const LogFile& lf, uint64_t data_size) {

    auto visible_set = computeVisibleSet(gi, si, segment_id, snapshot_versions);

    return filter(
        scan(lf, data_size),
        [vs = std::move(visible_set)](const EntryStream::Entry& e) -> bool {
            auto it = vs.find(e.hash);
            if (it == vs.end()) return false;
            uint32_t v = static_cast<uint32_t>(e.version);
            return it->second.count(v) > 0;
        });
}

std::unique_ptr<EntryStream> scanLatest(
    const GlobalIndex& gi, uint32_t segment_id,
    uint64_t snapshot_version,
    const LogFile& lf, uint64_t data_size) {

    return filter(
        scan(lf, data_size),
        [&gi, segment_id, snapshot_version](const EntryStream::Entry& e) -> bool {
            if (e.version > snapshot_version) {
                return false;
            }
            std::string key_str(e.key);
            uint64_t gi_version;
            uint32_t gi_segment_id;
            if (gi.get(key_str, snapshot_version, gi_version, gi_segment_id)) {
                return gi_segment_id == segment_id && gi_version == e.version;
            }
            return false;
        });
}

std::unique_ptr<EntryStream> scanLatestConsistent(
    const GlobalIndex& gi, uint32_t segment_id,
    uint64_t snapshot_version,
    const LogFile& lf, uint64_t data_size) {

    auto latest_set = computeLatestSet(gi, segment_id, snapshot_version);

    return filter(
        scan(lf, data_size),
        [ls = std::move(latest_set)](const EntryStream::Entry& e) -> bool {
            auto it = ls.find(e.hash);
            if (it == ls.end()) return false;
            return it->second == static_cast<uint32_t>(e.version);
        });
}

std::unique_ptr<EntryStream> scanWriteBuffer(
    const WriteBuffer& wb, uint64_t snapshot_version) {
    return wb.createStream(snapshot_version);
}

}  // namespace stream

}  // namespace internal
}  // namespace kvlite
