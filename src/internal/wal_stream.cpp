#include "internal/wal_stream.h"

#include <cstring>

#include "internal/wal.h"

namespace kvlite {
namespace internal {

// ---------------------------------------------------------------------------
// WALReplayStream
// ---------------------------------------------------------------------------

WALReplayStream::WALReplayStream(const std::string& wal_dir,
                                 const std::vector<uint32_t>& file_ids)
    : wal_dir_(wal_dir), file_ids_(file_ids) {
    // Load the first file that has records.
    while (file_idx_ < file_ids_.size()) {
        Status s = loadNextFile();
        if (!s.ok()) return; // leave valid_ = false on error
        if (!records_.empty()) {
            rec_idx_ = 0;
            updateCurrent();
            valid_ = true;
            return;
        }
        ++file_idx_;
    }
    // No records in any file.
}

bool WALReplayStream::valid() const { return valid_; }

const WALRecord& WALReplayStream::record() const { return current_; }

Status WALReplayStream::next() {
    if (!valid_) return Status::OK();

    ++rec_idx_;
    if (rec_idx_ < records_.size()) {
        updateCurrent();
        return Status::OK();
    }

    // Current file exhausted — try next files.
    ++file_idx_;
    while (file_idx_ < file_ids_.size()) {
        Status s = loadNextFile();
        if (!s.ok()) {
            valid_ = false;
            return s;
        }
        if (!records_.empty()) {
            rec_idx_ = 0;
            updateCurrent();
            return Status::OK();
        }
        ++file_idx_;
    }

    valid_ = false;
    return Status::OK();
}

Status WALReplayStream::loadNextFile() {
    records_.clear();
    rec_idx_ = 0;

    std::string path = GlobalIndexWAL::walFilePath(wal_dir_, file_ids_[file_idx_]);
    WAL wal;
    Status s = wal.open(path);
    if (!s.ok()) return s;

    // Accumulate records per transaction. When we see a kCommit, stamp all
    // preceding records with its version and flush into records_.
    struct TxnRecord {
        WalOp op;
        uint64_t packed_version;
        uint32_t segment_id;
        uint32_t new_segment_id;
        std::string key;
    };
    std::vector<TxnRecord> pending;

    uint64_t valid_end;
    s = wal.replay(
        [&](uint64_t /*seq*/,
            const std::vector<std::string_view>& recs) -> Status {
            pending.clear();
            uint64_t commit_version = 0;
            bool found_commit = false;

            for (const auto& rec : recs) {
                if (rec.size() < 1) continue;
                auto op = static_cast<WalOp>(static_cast<uint8_t>(rec[0]));
                const uint8_t* p = reinterpret_cast<const uint8_t*>(rec.data()) + 1;
                size_t remaining = rec.size() - 1;

                if (op == WalOp::kPut || op == WalOp::kEliminate) {
                    if (remaining < 14) continue;
                    uint64_t pv;
                    std::memcpy(&pv, p, 8); p += 8;
                    uint32_t seg;
                    std::memcpy(&seg, p, 4); p += 4;
                    uint16_t key_len;
                    std::memcpy(&key_len, p, 2); p += 2;
                    remaining -= 14;
                    if (remaining < key_len) continue;
                    std::string key(reinterpret_cast<const char*>(p), key_len);

                    pending.push_back({op, pv, seg, 0, std::move(key)});
                } else if (op == WalOp::kRelocate) {
                    if (remaining < 18) continue;
                    uint64_t pv;
                    std::memcpy(&pv, p, 8); p += 8;
                    uint32_t old_seg;
                    std::memcpy(&old_seg, p, 4); p += 4;
                    uint32_t new_seg;
                    std::memcpy(&new_seg, p, 4); p += 4;
                    uint16_t key_len;
                    std::memcpy(&key_len, p, 2); p += 2;
                    remaining -= 18;
                    if (remaining < key_len) continue;
                    std::string key(reinterpret_cast<const char*>(p), key_len);

                    pending.push_back({op, pv, old_seg, new_seg, std::move(key)});
                } else if (op == WalOp::kCommit) {
                    if (remaining < 8) continue;
                    std::memcpy(&commit_version, p, 8);
                    found_commit = true;
                }
            }

            // Stamp all pending records with the commit version and flush.
            if (found_commit) {
                for (auto& tr : pending) {
                    records_.push_back({tr.op, tr.packed_version, tr.segment_id,
                                        tr.new_segment_id, std::move(tr.key),
                                        commit_version});
                }
            }
            return Status::OK();
        },
        valid_end);

    if (!s.ok()) return s;
    return wal.close();
}

void WALReplayStream::updateCurrent() {
    const auto& r = records_[rec_idx_];
    current_.op = r.op;
    current_.packed_version = r.packed_version;
    current_.segment_id = r.segment_id;
    current_.new_segment_id = r.new_segment_id;
    current_.key = r.key;  // string_view into OwnedRecord's string
    current_.commit_version = r.commit_version;
}

// ---------------------------------------------------------------------------
// WALMergeStream
// ---------------------------------------------------------------------------

WALMergeStream::WALMergeStream(std::vector<std::unique_ptr<WALStream>> inputs)
    : inputs_(std::move(inputs)) {
    initHeap();
}

void WALMergeStream::initHeap() {
    for (auto& s : inputs_) {
        if (s && s->valid()) {
            heap_.push(s.get());
        }
    }
    if (!heap_.empty()) {
        current_stream_ = heap_.top();
    }
}

bool WALMergeStream::valid() const { return current_stream_ != nullptr; }

const WALRecord& WALMergeStream::record() const {
    return current_stream_->record();
}

Status WALMergeStream::next() {
    if (!current_stream_) return Status::OK();

    // Advance the stream that was just yielded.
    WALStream* top = heap_.top();
    heap_.pop();
    Status s = top->next();
    if (!s.ok()) {
        current_stream_ = nullptr;
        return s;
    }
    if (top->valid()) {
        heap_.push(top);
    }

    if (heap_.empty()) {
        current_stream_ = nullptr;
    } else {
        current_stream_ = heap_.top();
    }
    return Status::OK();
}

// ---------------------------------------------------------------------------
// Factory functions
// ---------------------------------------------------------------------------

namespace stream {

std::unique_ptr<WALStream> walReplay(
    const std::string& wal_dir,
    const std::vector<uint32_t>& file_ids) {
    return std::make_unique<WALReplayStream>(wal_dir, file_ids);
}

std::unique_ptr<WALStream> walMerge(
    std::vector<std::unique_ptr<WALStream>> inputs) {
    return std::make_unique<WALMergeStream>(std::move(inputs));
}

} // namespace stream

} // namespace internal
} // namespace kvlite
