#include "kvlite/db.h"

#include <memory>
#include <string>
#include <vector>

#include "internal/entry_stream.h"
#include "internal/global_index.h"
#include "internal/segment.h"
#include "internal/segment_storage_manager.h"
#include "internal/write_buffer.h"

namespace kvlite {

// --- Iterator::Impl ---
//
// Two-phase sequential scan with inline pertinence check.
//
// Phase 1 (segments): Scan each segment's log file sequentially. For each
// entry, check WriteBuffer precedence (skip if WB has a version for this key),
// then check GlobalIndex pertinence (emit only if this segment+version is the
// authoritative entry). No seen_keys tracking needed — the GI lookup naturally
// returns one (segment_id, version) match per key.
//
// Phase 2 (write buffer): Emit WB entries pertinent at the snapshot. The WB
// stream is already deduplicated (one entry per key, latest version <=
// snapshot). Skip tombstones.

class Iterator::Impl {
public:
    Impl(DB* db, Snapshot snapshot, bool owns_snapshot)
        : db_(db), snapshot_(snapshot), owns_snapshot_(owns_snapshot) {
        pinned_wb_ = db_->write_buffer_.get();
        pinned_wb_->pin();

        segment_ids_ = db_->storage_->getSegmentIds();
        for (uint32_t id : segment_ids_) {
            db_->storage_->pinSegment(id);
        }

        findNextValid();
    }

    ~Impl() {
        seg_stream_.reset();
        wb_stream_.reset();
        for (uint32_t id : segment_ids_) {
            db_->storage_->unpinSegment(id);
        }
        pinned_wb_->unpin();
        db_->cleanupRetiredBuffers();
        if (owns_snapshot_) {
            db_->releaseSnapshot(snapshot_);
        }
    }

    Status next(std::string& key, std::string& value, uint64_t& version) {
        if (!valid_) {
            return Status::NotFound("Iterator exhausted");
        }

        key = std::move(current_key_);
        value = std::move(current_value_);
        version = current_version_;

        findNextValid();
        return Status::OK();
    }

    const Snapshot& snapshot() const { return snapshot_; }

private:
    void findNextValid() {
        uint64_t snap_ver = snapshot_.version();

        // Phase 1 — scan segments sequentially with inline pertinence check.
        while (phase_ == 0) {
            if (!seg_stream_ || !seg_stream_->valid()) {
                if (!openNextSegment()) {
                    phase_ = 1;
                    wb_stream_ = internal::stream::scanWriteBuffer(
                        *pinned_wb_, snap_ver);
                    break;
                }
                continue;
            }

            // Read scalars from the entry ref before next() invalidates it.
            // Defer string copies: version check is free, key is needed for
            // WB/GI lookups, value is only needed on the emit path.
            const auto& e = seg_stream_->entry();
            uint64_t version = e.version;

            // Cheap scalar check — skip without any string copies.
            if (version > snap_ver) {
                seg_stream_->next();
                continue;
            }

            bool tombstone = e.tombstone;
            scan_key_.assign(e.key.data(), e.key.size());
            scan_value_.assign(e.value.data(), e.value.size());
            seg_stream_->next();

            // WB precedence: if WB has a version for this key at snapshot,
            // skip — it will be emitted in Phase 2.
            {
                uint64_t wver;
                bool wtomb;
                if (pinned_wb_->getByVersion(scan_key_, snap_ver,
                                             wb_probe_, wver, wtomb)) {
                    continue;
                }
            }

            // GI pertinence: only emit if this segment+version is the
            // pertinent entry for the key in the GlobalIndex.
            uint64_t gi_version;
            uint32_t gi_segment_id;
            if (!db_->global_index_->get(scan_key_, snap_ver,
                                         gi_version, gi_segment_id)) {
                continue;
            }
            if (gi_segment_id != current_segment_id_ ||
                gi_version != version) {
                continue;
            }

            if (tombstone) continue;

            // Pertinent non-tombstone — publish to current_*.
            current_key_.swap(scan_key_);
            current_value_.swap(scan_value_);
            current_version_ = version;
            valid_ = true;
            return;
        }

        // Phase 2 — emit WriteBuffer entries pertinent at the snapshot.
        while (wb_stream_ && wb_stream_->valid()) {
            const auto& e = wb_stream_->entry();

            if (e.tombstone) {
                wb_stream_->next();
                continue;
            }

            current_key_.assign(e.key.data(), e.key.size());
            current_value_.assign(e.value.data(), e.value.size());
            current_version_ = e.version;
            wb_stream_->next();

            valid_ = true;
            return;
        }

        valid_ = false;
    }

    bool openNextSegment() {
        seg_stream_.reset();
        while (seg_idx_ < segment_ids_.size()) {
            uint32_t id = segment_ids_[seg_idx_++];
            auto* seg = db_->storage_->getSegment(id);
            if (!seg) continue;
            current_segment_id_ = id;
            seg_stream_ = internal::stream::scan(
                seg->logFile(), seg->dataSize());
            if (seg_stream_->valid()) return true;
            seg_stream_.reset();
        }
        return false;
    }

    DB* db_;
    Snapshot snapshot_;
    bool owns_snapshot_;

    internal::WriteBuffer* pinned_wb_ = nullptr;
    std::vector<uint32_t> segment_ids_;

    // Phase 1: segments
    int phase_ = 0;
    size_t seg_idx_ = 0;
    uint32_t current_segment_id_ = 0;
    std::unique_ptr<internal::EntryStream> seg_stream_;

    // Phase 2: write buffer
    std::unique_ptr<internal::EntryStream> wb_stream_;

    // Reusable buffers — avoid per-entry allocations in the scan loop.
    std::string scan_key_;
    std::string scan_value_;
    std::string wb_probe_;  // sink for WB getByVersion value output

    bool valid_ = false;
    std::string current_key_;
    std::string current_value_;
    uint64_t current_version_ = 0;
};

// --- Iterator methods ---

Iterator::Iterator(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
Iterator::~Iterator() = default;

Iterator::Iterator(Iterator&& other) noexcept = default;
Iterator& Iterator::operator=(Iterator&& other) noexcept = default;

Status Iterator::next(std::string& key, std::string& value) {
    uint64_t version;
    return next(key, value, version);
}

Status Iterator::next(std::string& key, std::string& value, uint64_t& version) {
    return impl_->next(key, value, version);
}

const Snapshot& Iterator::snapshot() const {
    return impl_->snapshot();
}

// --- DB::createIterator ---

Status DB::createIterator(std::unique_ptr<Iterator>& iterator,
                          const ReadOptions& options) {
    if (!isOpen()) {
        return Status::InvalidArgument("Database not open");
    }

    if (options.snapshot) {
        auto impl = std::make_unique<Iterator::Impl>(
            this, *options.snapshot, /*owns_snapshot=*/false);
        iterator = std::unique_ptr<Iterator>(new Iterator(std::move(impl)));
    } else {
        auto impl = std::make_unique<Iterator::Impl>(
            this, createSnapshot(), /*owns_snapshot=*/true);
        iterator = std::unique_ptr<Iterator>(new Iterator(std::move(impl)));
    }
    return Status::OK();
}

}  // namespace kvlite
