#include "internal/gc_stream.h"

#include <algorithm>
#include <cassert>
#include <memory>
#include <queue>
#include <string>
#include <vector>

#include "internal/entry_stream.h"
#include "internal/log_entry.h"
#include "internal/version_dedup.h"

namespace kvlite {
namespace internal {
namespace {

// ---------------------------------------------------------------------------
// GCMergeStream — K-way merge over N EntryStreams in (hash asc, version asc)
// ---------------------------------------------------------------------------

class GCMergeStream : public EntryStream {
public:
    explicit GCMergeStream(std::vector<std::unique_ptr<EntryStream>> inputs)
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
            return Status::NotFound("GCMergeStream exhausted");
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
            return a->entry().version() > b->entry().version();
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
// GCDedupStream — writes EntryAction to ext[base + GCDedupExt::kAction]
//
// Input must be in (hash asc, version asc) order. Buffers entries per hash
// group, deduplicates the group when the hash changes (or stream exhausts),
// then replays the deduplicated entries one at a time.
//
// Dedup: for each snapshot version, the latest entry version <=
// snapshot is kept. All other entries are eliminated.
// ---------------------------------------------------------------------------

class GCDedupStream : public EntryStream {
public:
    GCDedupStream(std::unique_ptr<EntryStream> input,
                     std::vector<uint64_t> snapshot_versions,
                     size_t base)
        : input_(std::move(input)),
          snapshots_(std::move(snapshot_versions)),
          base_(base) {
        assert(base_ + GCDedupExt::kSize <= Entry::kMaxExt);
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
    // Per-entry metadata stored parallel to group_, capturing string
    // offsets into the arena so we can fix up string_views after
    // the arena is fully built (appending may reallocate).
    struct EntryMeta {
        size_t key_offset;
        size_t key_len;
        size_t value_offset;
        size_t value_len;
    };

    void fillGroup() {
        group_.clear();
        meta_.clear();
        arena_.clear();
        pos_ = 0;

        uint64_t current_hash = input_->entry().hash;

        // Collect all entries with the same hash.
        // Copy key+value into a contiguous arena (single allocation)
        // since input_->next() invalidates string_views.
        while (input_->valid() && input_->entry().hash == current_hash) {
            const auto& e = input_->entry();
            size_t key_off = arena_.size();
            arena_.append(e.key.data(), e.key.size());
            size_t val_off = arena_.size();
            arena_.append(e.value.data(), e.value.size());
            meta_.push_back({key_off, e.key.size(), val_off, e.value.size()});
            group_.push_back(e);
            Status s = input_->next();
            if (!s.ok()) break;
        }

        // Fix up string_views to point into the arena.
        for (size_t i = 0; i < group_.size(); ++i) {
            group_[i].key = std::string_view(
                arena_.data() + meta_[i].key_offset, meta_[i].key_len);
            group_[i].value = std::string_view(
                arena_.data() + meta_[i].value_offset, meta_[i].value_len);
        }

        // Dedup using shared two-pointer algorithm.
        size_t n = group_.size();
        versions_.resize(n);
        if (n > keep_cap_) {
            keep_ = std::make_unique<bool[]>(n);
            keep_cap_ = n;
        }
        for (size_t gi = 0; gi < n; ++gi) {
            versions_[gi] = group_[gi].version();
        }
        dedupVersionGroup(versions_.data(), n,
                             snapshots_, keep_.get());
        for (size_t gi = 0; gi < n; ++gi) {
            group_[gi].ext[base_ + GCDedupExt::kAction] =
                static_cast<uint64_t>(keep_[gi] ? EntryAction::kKeep
                                                : EntryAction::kEliminate);
        }
    }

    std::unique_ptr<EntryStream> input_;
    std::vector<uint64_t> snapshots_;
    size_t base_;
    std::vector<Entry> group_;
    std::string arena_;                  // contiguous key+value storage
    std::vector<EntryMeta> meta_;        // per-entry value offsets into arena
    std::vector<uint64_t> versions_;     // scratch: per-entry versions for dedup
    std::unique_ptr<bool[]> keep_;       // scratch: dedup output
    size_t keep_cap_ = 0;
    size_t pos_ = 0;
};

}  // anonymous namespace

// ---------------------------------------------------------------------------
// GC stream factory functions
// ---------------------------------------------------------------------------

namespace stream {

std::unique_ptr<EntryStream> gcMerge(std::vector<std::unique_ptr<EntryStream>> inputs) {
    return std::make_unique<GCMergeStream>(std::move(inputs));
}

std::unique_ptr<EntryStream> gcTagSource(
    std::unique_ptr<EntryStream> input, uint32_t segment_id, size_t base) {
    return std::make_unique<GCTagSourceStream>(std::move(input), segment_id, base);
}

std::unique_ptr<EntryStream> gcDedup(
    std::unique_ptr<EntryStream> input,
    const std::vector<uint64_t>& snapshot_versions,
    size_t base) {
    return std::make_unique<GCDedupStream>(std::move(input), snapshot_versions, base);
}

}  // namespace stream

}  // namespace internal
}  // namespace kvlite
