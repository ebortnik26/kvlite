#include "internal/segment_index.h"

#include <cstring>
#include <vector>

#include "internal/crc32.h"
#include "internal/log_file.h"

namespace kvlite {
namespace internal {

static constexpr uint32_t kMagic = 0x53494232;  // "SIB2"

// On-disk header — followed by raw arena + extension data + CRC32.
struct IndexHeader {
    uint32_t magic;
    uint32_t key_count;
    uint32_t entry_count;
    uint32_t ext_count;
    uint64_t arena_bytes;
};

static ReadOnlyDeltaHashTable::Config defaultDHTConfig() {
    ReadOnlyDeltaHashTable::Config config;
    config.bucket_bits = 16;
    config.bucket_bytes = 256;
    return config;
}

SegmentIndex::SegmentIndex() : dht_(defaultDHTConfig()) {}

SegmentIndex::~SegmentIndex() = default;
SegmentIndex::SegmentIndex(SegmentIndex&&) noexcept = default;
SegmentIndex& SegmentIndex::operator=(SegmentIndex&&) noexcept = default;

void SegmentIndex::put(uint64_t hash, uint32_t offset, uint64_t packed_version) {
    if (dht_.addEntryIsNew(hash, packed_version, offset)) {
        ++key_count_;
    }
}

void SegmentIndex::addBatchEntry(uint64_t hash, uint64_t packed_version, uint32_t offset) {
    dht_.addBatchEntry(hash, packed_version, offset);
}

void SegmentIndex::endBatch() {
    key_count_ = dht_.endBatch();
}

bool SegmentIndex::get(uint64_t hash,
                  std::vector<uint32_t>& offsets,
                  std::vector<uint64_t>& packed_versions) const {
    std::vector<uint64_t> pvs;
    std::vector<uint32_t> ids;
    if (!dht_.findAll(hash, pvs, ids)) return false;
    packed_versions = std::move(pvs);
    offsets = std::move(ids);
    return true;
}

bool SegmentIndex::get(uint64_t hash, uint64_t upper_bound,
                  uint64_t& offset, uint64_t& packed_version) const {
    uint32_t id;
    if (!dht_.findFirstBounded(hash, upper_bound, packed_version, id)) return false;
    offset = id;  // SegmentIndex stores offset in the id field
    return true;
}

bool SegmentIndex::getLatest(uint64_t hash,
                        uint32_t& offset, uint64_t& packed_version) const {
    uint64_t pv;
    uint32_t id;
    if (!dht_.findFirst(hash, pv, id)) return false;
    packed_version = pv;
    offset = id;
    return true;
}

bool SegmentIndex::contains(uint64_t hash) const {
    return dht_.contains(hash);
}

Status SegmentIndex::writeTo(LogFile& file) {
    dht_.seal();

    const size_t arena_bytes = dht_.arenaBytes();
    const uint32_t ext_count = dht_.extCount();
    const uint32_t stride = dht_.bucketStride();
    const size_t ext_bytes = static_cast<size_t>(ext_count) * stride;

    const size_t payload_size = sizeof(IndexHeader) + arena_bytes + ext_bytes;
    const size_t total_size = payload_size + sizeof(uint32_t);

    std::vector<uint8_t> buf(total_size);

    IndexHeader hdr{};
    hdr.magic = kMagic;
    hdr.key_count = static_cast<uint32_t>(key_count_);
    hdr.entry_count = static_cast<uint32_t>(dht_.size());
    hdr.ext_count = ext_count;
    hdr.arena_bytes = arena_bytes;
    std::memcpy(buf.data(), &hdr, sizeof(hdr));

    std::memcpy(buf.data() + sizeof(hdr), dht_.arenaData(), arena_bytes);

    if (ext_count > 0) {
        dht_.writeExtData(buf.data() + sizeof(hdr) + arena_bytes);
    }

    uint32_t checksum = crc32(buf.data(), payload_size);
    std::memcpy(buf.data() + payload_size, &checksum, sizeof(uint32_t));

    uint64_t write_offset;
    return file.append(buf.data(), total_size, write_offset);
}

Status SegmentIndex::readFrom(const LogFile& file, uint64_t offset) {
    IndexHeader hdr;
    Status s = file.readAt(offset, &hdr, sizeof(hdr));
    if (!s.ok()) return s;

    if (hdr.magic != kMagic) {
        return Status::Corruption("SegmentIndex: bad magic");
    }

    const uint32_t stride = dht_.bucketStride();
    const size_t ext_bytes = static_cast<size_t>(hdr.ext_count) * stride;
    const size_t payload_size = sizeof(IndexHeader) + hdr.arena_bytes + ext_bytes;
    const size_t total_size = payload_size + sizeof(uint32_t);

    std::vector<uint8_t> buf(total_size);
    s = file.readAt(offset, buf.data(), total_size);
    if (!s.ok()) return s;

    uint32_t stored_crc;
    std::memcpy(&stored_crc, buf.data() + payload_size, sizeof(uint32_t));
    if (crc32(buf.data(), payload_size) != stored_crc) {
        return Status::ChecksumMismatch("SegmentIndex: checksum mismatch");
    }

    clear();

    const uint8_t* arena_data = buf.data() + sizeof(IndexHeader);
    const uint8_t* ext_data = arena_data + hdr.arena_bytes;

    dht_.loadBinary(arena_data, hdr.arena_bytes,
                    ext_data, hdr.ext_count,
                    hdr.entry_count);
    key_count_ = hdr.key_count;

    return Status::OK();
}

void SegmentIndex::forEach(const std::function<void(uint32_t offset, uint64_t packed_version)>& fn) const {
    dht_.forEach([&fn](uint64_t /*hash*/, uint64_t packed_version, uint32_t id) {
        fn(id, packed_version);
    });
}

void SegmentIndex::forEachGroup(
    const std::function<void(uint64_t hash,
                             const std::vector<uint32_t>& offsets,
                             const std::vector<uint64_t>& packed_versions)>& fn) const {
    dht_.forEachGroup([&fn](uint64_t hash,
                            const std::vector<uint64_t>& pvs,
                            const std::vector<uint32_t>& ids) {
        fn(hash, ids, pvs);
    });
}

size_t SegmentIndex::keyCount() const {
    return key_count_;
}

size_t SegmentIndex::entryCount() const {
    return dht_.size();
}

size_t SegmentIndex::memoryUsage() const {
    return dht_.memoryUsage();
}

void SegmentIndex::clear() {
    dht_.clear();
    key_count_ = 0;
}

}  // namespace internal
}  // namespace kvlite
