#include "internal/entry_stream.h"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <memory>
#include <set>
#include <string>

#include "internal/crc32.h"
#include "internal/delta_hash_table_base.h"
#include "internal/global_index.h"
#include "internal/log_entry.h"
#include "internal/log_file.h"
#include "internal/write_buffer.h"

namespace kvlite {
namespace internal {

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
// GCTagSourceStream — writes segment_id to ext[base + GCTagSourceExt::kSegmentId]
// ---------------------------------------------------------------------------

class GCTagSourceStream : public EntryStream {
public:
    GCTagSourceStream(std::unique_ptr<EntryStream> input, uint32_t segment_id, size_t base)
        : input_(std::move(input)), segment_id_(segment_id), base_(base) {
        assert(base_ + GCTagSourceExt::kSize <= Entry::kMaxExt);
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
        current_.ext[base_ + GCTagSourceExt::kSegmentId] = segment_id_;
    }

    std::unique_ptr<EntryStream> input_;
    uint32_t segment_id_;
    size_t base_;
    Entry current_;
};

// ---------------------------------------------------------------------------
// GCClassifyStream — writes EntryAction to ext[base + GCClassifyExt::kAction]
//
// Input must be in (hash asc, version asc) order. Buffers entries per hash
// group, classifies the group when the hash changes (or stream exhausts),
// then replays the classified entries one at a time.
//
// Classification: for each snapshot version, the latest entry version <=
// snapshot is kept. All other entries are eliminated.
// ---------------------------------------------------------------------------

class GCClassifyStream : public EntryStream {
public:
    GCClassifyStream(std::unique_ptr<EntryStream> input,
                     std::vector<uint64_t> snapshot_versions,
                     size_t base)
        : input_(std::move(input)),
          snapshots_(std::move(snapshot_versions)),
          base_(base) {
        assert(base_ + GCClassifyExt::kSize <= Entry::kMaxExt);
        std::sort(snapshots_.begin(), snapshots_.end());
        if (input_->valid()) {
            fillGroup();
        }
    }

    bool valid() const override { return pos_ < group_.size(); }
    const Entry& entry() const override { return group_[pos_]; }

    Status next() override {
        ++pos_;
        if (pos_ < group_.size()) {
            return Status::OK();
        }
        // Current group exhausted — fill next group from input.
        if (input_->valid()) {
            fillGroup();
        }
        return Status::OK();
    }

private:
    // Consume entries from input_ sharing the same hash, classify them,
    // and store in group_. Leaves input_ positioned at the first entry
    // of the next hash group (or exhausted).
    void fillGroup() {
        group_.clear();
        strings_.clear();
        pos_ = 0;

        uint64_t current_hash = input_->entry().hash;

        // Collect all entries with the same hash.
        // Copy key/value into owned strings since input_->next() invalidates
        // the string_views from the previous entry.
        while (input_->valid() && input_->entry().hash == current_hash) {
            const auto& e = input_->entry();
            strings_.emplace_back(e.key);
            strings_.emplace_back(e.value);
            group_.push_back(e);
            Status s = input_->next();
            if (!s.ok()) break;
        }

        // Fix up string_views to point at owned strings.
        for (size_t i = 0; i < group_.size(); ++i) {
            group_[i].key = strings_[i * 2];
            group_[i].value = strings_[i * 2 + 1];
        }

        // Classify: entries arrive in version-asc order.
        // For each snapshot, the latest version <= snapshot is kept.
        std::set<size_t> keep_indices;
        for (uint64_t snap : snapshots_) {
            // Walk backwards to find latest version <= snap.
            for (int i = static_cast<int>(group_.size()) - 1; i >= 0; --i) {
                if (group_[i].version <= snap) {
                    keep_indices.insert(static_cast<size_t>(i));
                    break;
                }
            }
        }

        for (size_t i = 0; i < group_.size(); ++i) {
            group_[i].ext[base_ + GCClassifyExt::kAction] =
                keep_indices.count(i)
                    ? static_cast<uint64_t>(EntryAction::kKeep)
                    : static_cast<uint64_t>(EntryAction::kEliminate);
        }
    }

    std::unique_ptr<EntryStream> input_;
    std::vector<uint64_t> snapshots_;
    size_t base_;
    std::vector<Entry> group_;
    std::vector<std::string> strings_;  // owned key/value storage
    size_t pos_ = 0;
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

std::unique_ptr<EntryStream> gcTagSource(
    std::unique_ptr<EntryStream> input, uint32_t segment_id, size_t base) {
    return std::make_unique<GCTagSourceStream>(std::move(input), segment_id, base);
}

std::unique_ptr<EntryStream> gcClassify(
    std::unique_ptr<EntryStream> input,
    const std::vector<uint64_t>& snapshot_versions,
    size_t base) {
    return std::make_unique<GCClassifyStream>(std::move(input), snapshot_versions, base);
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

std::unique_ptr<EntryStream> scanWriteBuffer(
    const WriteBuffer& wb, uint64_t snapshot_version) {
    return wb.createStream(snapshot_version);
}

}  // namespace stream

}  // namespace internal
}  // namespace kvlite
