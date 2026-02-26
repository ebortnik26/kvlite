#include "internal/wal_stream.h"

#include <cstring>
#include <map>

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

    // Per-producer pending buffers for demultiplexing. Within a single WAL
    // transaction, records from different producers can be interleaved, but
    // each producer's kCommit flushes only that producer's pending records.
    // Replay order matches WAL order (append-only ⇒ chronological), so no
    // cross-producer sorting is needed.
    struct TxnRecord {
        WalOp op;
        uint8_t producer_id;
        uint64_t packed_version;
        uint32_t segment_id;
        uint32_t new_segment_id;
        std::string key;
    };
    std::map<uint8_t, std::vector<TxnRecord>> pending_by_producer;

    uint64_t valid_end;
    s = wal.replay(
        [&](uint64_t /*seq*/,
            const std::vector<std::string_view>& recs) -> Status {

            for (const auto& rec : recs) {
                if (rec.size() < 2) continue;  // need at least op + producer_id
                auto op = static_cast<WalOp>(static_cast<uint8_t>(rec[0]));
                uint8_t producer_id = static_cast<uint8_t>(rec[1]);
                const uint8_t* p = reinterpret_cast<const uint8_t*>(rec.data()) + 2;
                size_t remaining = rec.size() - 2;

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

                    pending_by_producer[producer_id].push_back(
                        {op, producer_id, pv, seg, 0, std::move(key)});
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

                    pending_by_producer[producer_id].push_back(
                        {op, producer_id, pv, old_seg, new_seg, std::move(key)});
                } else if (op == WalOp::kCommit) {
                    if (remaining < 8) continue;
                    uint64_t commit_version;
                    std::memcpy(&commit_version, p, 8);

                    // Flush only this producer's pending records into records_.
                    auto it = pending_by_producer.find(producer_id);
                    if (it != pending_by_producer.end() && !it->second.empty()) {
                        for (auto& tr : it->second) {
                            records_.push_back({tr.op, tr.producer_id,
                                                tr.packed_version, tr.segment_id,
                                                tr.new_segment_id, std::move(tr.key),
                                                commit_version});
                        }
                        it->second.clear();
                    }
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
    current_.producer_id = r.producer_id;
    current_.packed_version = r.packed_version;
    current_.segment_id = r.segment_id;
    current_.new_segment_id = r.new_segment_id;
    current_.key = r.key;  // string_view into OwnedRecord's string
    current_.commit_version = r.commit_version;
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

} // namespace stream

} // namespace internal
} // namespace kvlite
