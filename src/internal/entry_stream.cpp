#include "internal/entry_stream.h"

#include <cstring>
#include <memory>
#include <string>

#include "internal/crc32.h"
#include "internal/delta_hash_table.h"
#include "internal/log_entry.h"
#include "internal/log_file.h"
#include "internal/memtable.h"

namespace kvlite {
namespace internal {

// ---------------------------------------------------------------------------
// ScanStream — reads ALL entries from a LogFile using 1 MB read-ahead
// ---------------------------------------------------------------------------
//
// Reads the data region in large chunks (~1 MB) and parses entries out of
// the in-memory buffer. This collapses thousands of per-entry pread syscalls
// into a handful of large sequential reads, which is critical for GC and
// Iterator performance.  When an entry straddles the buffer boundary, the
// overlapping tail is compacted to the front and the next chunk is appended.

class ScanStream : public EntryStream {
public:
    static constexpr size_t kReadAhead = 1u << 20;  // 1 MB

    ScanStream(const LogFile& log_file, uint64_t data_size)
        : log_file_(log_file), data_size_(data_size) {
        buf_.resize(kReadAhead);
        if (data_size_ > 0) {
            Status s = fillBuffer();
            if (s.ok()) readNext();
        }
    }

    bool valid() const override { return valid_; }
    const Entry& entry() const override { return current_; }

    Status next() override {
        return readNext();
    }

private:
    // Fill (or refill) the read-ahead buffer starting from file_offset_.
    // Preserves any unconsumed bytes at the front.
    Status fillBuffer() {
        // Move unconsumed data to the front.
        size_t unconsumed = buf_end_ - buf_pos_;
        if (unconsumed > 0 && buf_pos_ > 0) {
            std::memmove(buf_.data(), buf_.data() + buf_pos_, unconsumed);
        }
        buf_pos_ = 0;
        buf_end_ = unconsumed;

        // Read as much as will fit in the buffer.
        size_t remaining_file = (file_offset_ < data_size_)
            ? static_cast<size_t>(data_size_ - file_offset_) : 0;
        size_t to_read = std::min(kReadAhead - unconsumed, remaining_file);
        if (to_read == 0) return Status::OK();

        // Ensure the buffer can hold unconsumed + new data.
        if (unconsumed + to_read > buf_.size()) {
            buf_.resize(unconsumed + to_read);
        }

        Status s = log_file_.readAt(file_offset_, buf_.data() + unconsumed, to_read);
        if (!s.ok()) return s;

        buf_end_ = unconsumed + to_read;
        file_offset_ += to_read;
        return Status::OK();
    }

    Status readNext() {
        size_t avail = buf_end_ - buf_pos_;

        // Need at least a header to continue.
        if (avail < LogEntry::kHeaderSize) {
            if (file_offset_ >= data_size_ && avail == 0) {
                valid_ = false;
                return Status::OK();
            }
            Status s = fillBuffer();
            if (!s.ok()) { valid_ = false; return s; }
            avail = buf_end_ - buf_pos_;
            if (avail < LogEntry::kHeaderSize) {
                valid_ = false;
                if (avail == 0) return Status::OK();
                return Status::Corruption("ScanStream: truncated header at offset " +
                                          std::to_string(file_offset_ - avail));
            }
        }

        // Parse header from the buffer.
        const uint8_t* hdr = buf_.data() + buf_pos_;
        uint64_t packed_ver;
        uint16_t kl;
        uint32_t vl;
        std::memcpy(&packed_ver, hdr, 8);
        std::memcpy(&kl, hdr + 8, 2);
        std::memcpy(&vl, hdr + 10, 4);

        size_t entry_size = LogEntry::kHeaderSize + kl + vl + LogEntry::kChecksumSize;

        // If the full entry isn't in the buffer, refill and retry.
        if (entry_size > avail) {
            // Grow buffer if entry is larger than kReadAhead.
            if (entry_size > buf_.size()) {
                buf_.resize(entry_size);
            }
            Status s = fillBuffer();
            if (!s.ok()) { valid_ = false; return s; }
            avail = buf_end_ - buf_pos_;
            if (entry_size > avail) {
                valid_ = false;
                return Status::Corruption("ScanStream: truncated entry at offset " +
                                          std::to_string(file_offset_ - avail));
            }
            hdr = buf_.data() + buf_pos_;
        }

        // Validate CRC.
        size_t payload_len = LogEntry::kHeaderSize + kl + vl;
        uint32_t stored_crc;
        std::memcpy(&stored_crc, hdr + payload_len, 4);
        uint32_t computed_crc = crc32(hdr, payload_len);
        if (stored_crc != computed_crc) {
            valid_ = false;
            return Status::Corruption("ScanStream: CRC mismatch at offset " +
                                      std::to_string(file_offset_ - avail));
        }

        const char* key_ptr = reinterpret_cast<const char*>(hdr + LogEntry::kHeaderSize);
        const char* val_ptr = key_ptr + kl;

        current_.hash = dhtHashBytes(key_ptr, kl);
        current_.key = std::string_view(key_ptr, kl);
        current_.value = std::string_view(val_ptr, vl);
        current_.pv = PackedVersion(packed_ver);
        valid_ = true;

        buf_pos_ += entry_size;
        return Status::OK();
    }

    const LogFile& log_file_;
    uint64_t data_size_;
    uint64_t file_offset_ = 0;  // next byte to read from file
    std::vector<uint8_t> buf_;  // read-ahead buffer; string_views point into it
    size_t buf_pos_ = 0;        // cursor: next unprocessed byte in buf_
    size_t buf_end_ = 0;        // one past last valid byte in buf_
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
// Factory functions
// ---------------------------------------------------------------------------

namespace stream {

std::unique_ptr<EntryStream> scan(const LogFile& lf, uint64_t data_size) {
    return std::make_unique<ScanStream>(lf, data_size);
}

std::unique_ptr<EntryStream> filter(std::unique_ptr<EntryStream> input, Predicate pred) {
    return std::make_unique<FilterStream>(std::move(input), std::move(pred));
}

std::unique_ptr<EntryStream> scanMemtable(
    const Memtable& mt, uint64_t snapshot_version) {
    return mt.createStream(snapshot_version);
}

}  // namespace stream

}  // namespace internal
}  // namespace kvlite
