#include "internal/segment.h"

#include <cstring>

namespace kvlite {
namespace internal {

Segment::Segment() = default;
Segment::~Segment() = default;
Segment::Segment(Segment&&) noexcept = default;
Segment& Segment::operator=(Segment&&) noexcept = default;

// --- Lifecycle ---

Status Segment::create(const std::string& path) {
    if (state_ != State::kClosed) {
        return Status::InvalidArgument("Segment: create requires Closed state");
    }
    Status s = log_file_.create(path);
    if (s.ok()) {
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

    // Read footer: [index_offset:8][magic:4]
    uint8_t footer[kFooterSize];
    s = log_file_.readAt(file_size - kFooterSize, footer, kFooterSize);
    if (!s.ok()) {
        log_file_.close();
        return s;
    }

    uint64_t index_offset;
    uint32_t magic;
    std::memcpy(&index_offset, footer, 8);
    std::memcpy(&magic, footer + 8, 4);

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

    // Append serialized L2 index.
    Status s = index_.writeTo(log_file_);
    if (!s.ok()) return s;

    // Append footer: [index_offset:8][magic:4]
    uint8_t footer[kFooterSize];
    std::memcpy(footer, &data_size_, 8);
    uint32_t magic = kFooterMagic;
    std::memcpy(footer + 8, &magic, 4);

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

Status Segment::append(const void* data, size_t len, uint64_t& offset) {
    if (state_ != State::kWriting) {
        return Status::InvalidArgument("Segment: append requires Writing state");
    }
    Status s = log_file_.append(data, len, offset);
    if (s.ok()) {
        data_size_ = offset + len;
    }
    return s;
}

Status Segment::addIndex(const std::string& key, uint32_t offset, uint32_t version) {
    if (state_ != State::kWriting) {
        return Status::InvalidArgument("Segment: addIndex requires Writing state");
    }
    index_.put(key, offset, version);
    return Status::OK();
}

// --- Read ---

Status Segment::readAt(uint64_t offset, void* buf, size_t len) {
    if (state_ != State::kReadable) {
        return Status::InvalidArgument("Segment: readAt requires Readable state");
    }
    return log_file_.readAt(offset, buf, len);
}

// --- Index queries ---

bool Segment::getLatest(const std::string& key,
                        uint32_t& offset, uint32_t& version) const {
    if (state_ != State::kReadable) return false;
    return index_.getLatest(key, offset, version);
}

bool Segment::get(const std::string& key,
                  std::vector<uint32_t>& offsets,
                  std::vector<uint32_t>& versions) const {
    if (state_ != State::kReadable) return false;
    return index_.get(key, offsets, versions);
}

bool Segment::get(const std::string& key, uint64_t upper_bound,
                  uint64_t& offset, uint64_t& version) const {
    if (state_ != State::kReadable) return false;
    return index_.get(key, upper_bound, offset, version);
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

}  // namespace internal
}  // namespace kvlite
