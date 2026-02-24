#include "internal/entry_stream.h"

#include <cstring>
#include <memory>
#include <string>

#include "internal/crc32.h"
#include "internal/delta_hash_table_base.h"
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
// Factory functions
// ---------------------------------------------------------------------------

namespace stream {

std::unique_ptr<EntryStream> scan(const LogFile& lf, uint64_t data_size) {
    return std::make_unique<ScanStream>(lf, data_size);
}

std::unique_ptr<EntryStream> filter(std::unique_ptr<EntryStream> input, Predicate pred) {
    return std::make_unique<FilterStream>(std::move(input), std::move(pred));
}

std::unique_ptr<EntryStream> scanWriteBuffer(
    const WriteBuffer& wb, uint64_t snapshot_version) {
    return wb.createStream(snapshot_version);
}

}  // namespace stream

}  // namespace internal
}  // namespace kvlite
