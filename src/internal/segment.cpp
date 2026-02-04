#include "internal/segment.h"

namespace kvlite {
namespace internal {

Segment::Segment() = default;
Segment::~Segment() = default;
Segment::Segment(Segment&&) noexcept = default;
Segment& Segment::operator=(Segment&&) noexcept = default;

// --- Lifecycle ---

Status Segment::create(const std::string& path) {
    return log_file_.create(path);
}

Status Segment::load(const std::string& data_path,
                     const std::string& index_path) {
    Status s = log_file_.open(data_path);
    if (!s.ok()) return s;

    LogFile idx_file;
    s = idx_file.open(index_path);
    if (!s.ok()) {
        log_file_.close();
        return s;
    }

    s = index_.readFrom(idx_file);
    if (!s.ok()) {
        log_file_.close();
        return s;
    }

    return Status::OK();
}

Status Segment::saveIndex(const std::string& path) {
    LogFile idx_file;
    Status s = idx_file.create(path);
    if (!s.ok()) return s;

    s = index_.writeTo(idx_file);
    if (!s.ok()) return s;

    return idx_file.close();
}

Status Segment::close() {
    return log_file_.close();
}

bool Segment::isOpen() const {
    return log_file_.isOpen();
}

// --- Write ---

Status Segment::append(const void* data, size_t len, uint64_t& offset) {
    return log_file_.append(data, len, offset);
}

void Segment::addIndex(const std::string& key, uint32_t offset, uint32_t version) {
    index_.put(key, offset, version);
}

// --- Read ---

Status Segment::readAt(uint64_t offset, void* buf, size_t len) {
    return log_file_.readAt(offset, buf, len);
}

// --- Index queries ---

bool Segment::getLatest(const std::string& key,
                        uint32_t& offset, uint32_t& version) const {
    return index_.getLatest(key, offset, version);
}

bool Segment::get(const std::string& key,
                  std::vector<uint32_t>& offsets,
                  std::vector<uint32_t>& versions) const {
    return index_.get(key, offsets, versions);
}

bool Segment::get(const std::string& key, uint64_t upper_bound,
                  uint64_t& offset, uint64_t& version) const {
    return index_.get(key, upper_bound, offset, version);
}

bool Segment::contains(const std::string& key) const {
    return index_.contains(key);
}

// --- Stats ---

uint64_t Segment::size() const {
    return log_file_.size();
}

size_t Segment::keyCount() const {
    return index_.keyCount();
}

size_t Segment::entryCount() const {
    return index_.entryCount();
}

}  // namespace internal
}  // namespace kvlite
