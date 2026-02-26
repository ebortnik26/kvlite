#include "internal/segment.h"

#include <cstring>
#include <memory>

namespace kvlite {
namespace internal {

Segment::Segment() = default;
Segment::~Segment() = default;
Segment::Segment(Segment&&) noexcept = default;
Segment& Segment::operator=(Segment&&) noexcept = default;

// --- Lifecycle ---

Status Segment::create(const std::string& path, uint32_t id) {
    if (state_ != State::kClosed) {
        return Status::InvalidArgument("Segment: create requires Closed state");
    }
    Status s = log_file_.create(path);
    if (s.ok()) {
        id_ = id;
        data_size_ = 0;
        state_ = State::kWriting;
    }
    return s;
}

Status Segment::open(const std::string& path) {
    if (state_ != State::kClosed) {
        return Status::InvalidArgument("Segment: open requires Closed state");
    }
    Status s = log_file_.open(path);
    if (!s.ok()) return s;

    uint64_t file_size = log_file_.size();
    if (file_size < kFooterSize) {
        log_file_.close();
        return Status::Corruption("Segment: file too small for footer");
    }

    // Read footer: [segment_id:4][index_offset:8][magic:4]
    uint8_t footer[kFooterSize];
    s = log_file_.readAt(file_size - kFooterSize, footer, kFooterSize);
    if (!s.ok()) {
        log_file_.close();
        return s;
    }

    uint32_t id;
    uint64_t index_offset;
    uint32_t magic;
    std::memcpy(&id, footer, 4);
    std::memcpy(&index_offset, footer + 4, 8);
    std::memcpy(&magic, footer + 12, 4);

    if (magic != kFooterMagic) {
        log_file_.close();
        return Status::Corruption("Segment: bad footer magic");
    }

    if (index_offset > file_size - kFooterSize) {
        log_file_.close();
        return Status::Corruption("Segment: invalid index offset");
    }

    s = index_.readFrom(log_file_, index_offset);
    if (!s.ok()) {
        log_file_.close();
        return s;
    }

    id_ = id;
    data_size_ = index_offset;
    state_ = State::kReadable;
    return Status::OK();
}

Status Segment::seal() {
    if (state_ != State::kWriting) {
        return Status::InvalidArgument("Segment: seal requires Writing state");
    }

    // Record where the data region ends / index begins.
    data_size_ = log_file_.size();

    // Append serialized SegmentIndex.
    Status s = index_.writeTo(log_file_);
    if (!s.ok()) return s;

    // Append footer: [segment_id:4][index_offset:8][magic:4]
    uint8_t footer[kFooterSize];
    std::memcpy(footer, &id_, 4);
    std::memcpy(footer + 4, &data_size_, 8);
    uint32_t magic = kFooterMagic;
    std::memcpy(footer + 12, &magic, 4);

    uint64_t footer_offset;
    s = log_file_.append(footer, kFooterSize, footer_offset);
    if (!s.ok()) return s;

    state_ = State::kReadable;
    return Status::OK();
}

Status Segment::close() {
    if (state_ == State::kClosed) {
        return Status::OK();
    }
    Status s = log_file_.close();
    state_ = State::kClosed;
    return s;
}

// --- Write ---

Status Segment::put(std::string_view key, uint64_t version,
                    std::string_view value, bool tombstone) {
    if (state_ != State::kWriting) {
        return Status::InvalidArgument("Segment: put requires Writing state");
    }
    if (key.size() > LogEntry::kMaxKeyLen) {
        return Status::InvalidArgument("Segment: key too long");
    }

    // On-disk version is the packed form: (logical_version << 1) | tombstone.
    PackedVersion pv(version, tombstone);
    uint64_t packed_ver = pv.data;
    uint16_t key_len = static_cast<uint16_t>(key.size());
    uint32_t val_len = static_cast<uint32_t>(value.size());
    size_t raw_key_len = key.size();
    size_t entry_size = LogEntry::kHeaderSize + raw_key_len + val_len +
                        LogEntry::kChecksumSize;

    uint8_t stack_buf[4096];
    std::unique_ptr<uint8_t[]> heap_buf;
    uint8_t* buf = stack_buf;
    if (entry_size > sizeof(stack_buf)) {
        heap_buf = std::make_unique<uint8_t[]>(entry_size);
        buf = heap_buf.get();
    }

    uint8_t* p = buf;
    std::memcpy(p, &packed_ver, 8); p += 8;
    std::memcpy(p, &key_len, 2);    p += 2;
    std::memcpy(p, &val_len, 4);    p += 4;
    std::memcpy(p, key.data(), raw_key_len);   p += raw_key_len;
    std::memcpy(p, value.data(), val_len);  p += val_len;

    size_t payload_len = LogEntry::kHeaderSize + raw_key_len + val_len;
    uint32_t checksum = crc32(buf, payload_len);
    std::memcpy(p, &checksum, 4);

    uint64_t offset;
    Status s = log_file_.append(buf, entry_size, offset);
    if (!s.ok()) return s;

    data_size_ = offset + entry_size;
    // Store packed version in index so tombstone info is preserved.
    index_.put(key, static_cast<uint32_t>(offset), packed_ver);
    return Status::OK();
}

// --- Read ---

Status Segment::readEntry(uint64_t offset, LogEntry& entry) const {
    // Speculative read: fetch up to 4KB in one I/O.  Most entries (header +
    // key + value + CRC) fit entirely, so this avoids a second pread.
    static constexpr size_t kReadAhead = 4096;
    uint8_t stack_buf[kReadAhead];

    // Clamp to bytes remaining in the data region to avoid reading past EOF.
    size_t avail = (offset < data_size_) ? static_cast<size_t>(data_size_ - offset) : 0;
    size_t first_read = (avail < kReadAhead) ? avail : kReadAhead;
    if (first_read < LogEntry::kHeaderSize + LogEntry::kChecksumSize) {
        return Status::Corruption("Segment: truncated entry at offset " +
                                  std::to_string(offset));
    }
    Status s = log_file_.readAt(offset, stack_buf, first_read);
    if (!s.ok()) return s;

    // Parse header from the speculative buffer.
    uint64_t packed_ver;
    uint16_t kl;
    uint32_t vl;
    std::memcpy(&packed_ver, stack_buf, 8);
    std::memcpy(&kl, stack_buf + 8, 2);
    std::memcpy(&vl, stack_buf + 10, 4);

    size_t entry_size = LogEntry::kHeaderSize + kl + vl + LogEntry::kChecksumSize;

    // If the entry fits in the speculative buffer, use it directly (one I/O).
    // Otherwise fall back to a heap allocation + second pread.
    uint8_t* buf = stack_buf;
    std::unique_ptr<uint8_t[]> heap_buf;
    if (entry_size > first_read) {
        heap_buf = std::make_unique<uint8_t[]>(entry_size);
        buf = heap_buf.get();
        s = log_file_.readAt(offset, buf, entry_size);
        if (!s.ok()) return s;
    }

    // Validate CRC.
    size_t payload_len = LogEntry::kHeaderSize + kl + vl;
    uint32_t stored_crc;
    std::memcpy(&stored_crc, buf + payload_len, 4);
    uint32_t computed_crc = crc32(buf, payload_len);
    if (stored_crc != computed_crc) {
        return Status::Corruption("Segment: CRC mismatch at offset " +
                                  std::to_string(offset));
    }

    entry.pv = PackedVersion(packed_ver);
    entry.key.assign(reinterpret_cast<const char*>(buf + LogEntry::kHeaderSize), kl);
    entry.value.assign(reinterpret_cast<const char*>(buf + LogEntry::kHeaderSize + kl), vl);
    return Status::OK();
}

Status Segment::getLatest(const std::string& key, LogEntry& entry) const {
    if (state_ != State::kReadable) {
        return Status::InvalidArgument("Segment: getLatest requires Readable state");
    }
    uint32_t offset;
    uint64_t packed_version;
    if (!index_.getLatest(key, offset, packed_version)) {
        return Status::NotFound("key not found");
    }
    return readEntry(offset, entry);
}

Status Segment::get(const std::string& key,
                    std::vector<LogEntry>& entries) const {
    if (state_ != State::kReadable) {
        return Status::InvalidArgument("Segment: get requires Readable state");
    }
    std::vector<uint32_t> offsets;
    std::vector<uint64_t> packed_versions;
    if (!index_.get(key, offsets, packed_versions)) {
        return Status::NotFound("key not found");
    }
    entries.clear();
    entries.reserve(offsets.size());
    for (size_t i = 0; i < offsets.size(); ++i) {
        LogEntry entry;
        Status s = readEntry(offsets[i], entry);
        if (!s.ok()) return s;
        entries.push_back(std::move(entry));
    }
    return Status::OK();
}

Status Segment::get(const std::string& key, uint64_t upper_bound,
                    LogEntry& entry) const {
    if (state_ != State::kReadable) {
        return Status::InvalidArgument("Segment: get requires Readable state");
    }
    uint64_t offset, packed_version;
    if (!index_.get(key, upper_bound, offset, packed_version)) {
        return Status::NotFound("key not found");
    }
    return readEntry(offset, entry);
}

Status Segment::readTombstone(uint64_t offset, bool& tombstone) const {
    // Tombstone is the LSB of the packed version (first 8 bytes).
    // Only need 1 byte, but read 8 for alignment safety.
    uint64_t packed_ver;
    Status s = log_file_.readAt(offset, &packed_ver, 8);
    if (!s.ok()) return s;
    tombstone = (packed_ver & PackedVersion::kTombstoneMask) != 0;
    return Status::OK();
}

bool Segment::contains(const std::string& key) const {
    if (state_ != State::kReadable) return false;
    return index_.contains(key);
}

// --- Stats ---

uint64_t Segment::dataSize() const {
    return data_size_;
}

size_t Segment::keyCount() const {
    return index_.keyCount();
}

size_t Segment::entryCount() const {
    return index_.entryCount();
}

Status Segment::readKeyByVersion(uint64_t packed_version, std::string& key) const {
    size_t offset = 0;
    while (offset < data_size_) {
        uint8_t hdr[LogEntry::kHeaderSize];
        Status s = log_file_.readAt(offset, hdr, LogEntry::kHeaderSize);
        if (!s.ok()) return s;

        uint64_t pv;
        std::memcpy(&pv, hdr, 8);
        uint16_t kl;
        std::memcpy(&kl, hdr + 8, 2);
        uint32_t vl;
        std::memcpy(&vl, hdr + 10, 4);

        if (pv == packed_version) {
            key.resize(kl);
            s = log_file_.readAt(offset + LogEntry::kHeaderSize,
                                 key.data(), kl);
            return s;
        }
        offset += LogEntry::kHeaderSize + kl + vl + LogEntry::kChecksumSize;
    }
    return Status::NotFound("Version not found in segment");
}

}  // namespace internal
}  // namespace kvlite
