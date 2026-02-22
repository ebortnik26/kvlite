#include "internal/segment_index.h"

#include <cstring>
#include <vector>

#include "internal/crc32.h"
#include "internal/log_file.h"

namespace kvlite {
namespace internal {

static const uint32_t kSegmentIndexMagic = 0x4C324958;  // "L2IX" (legacy)

struct SegmentIndexHeader {
    uint32_t magic;
    uint32_t entry_count;
    uint32_t key_count;
    uint32_t reserved;
};

struct SegmentIndexEntry {
    uint64_t hash;
    uint32_t offset;
    uint32_t version;
};

static SegmentDeltaHashTable::Config defaultDHTConfig() {
    SegmentDeltaHashTable::Config config;
    config.bucket_bits = 16;
    config.lslot_bits = 4;
    config.bucket_bytes = 256;
    return config;
}

SegmentIndex::SegmentIndex() : dht_(defaultDHTConfig()) {}

SegmentIndex::~SegmentIndex() = default;
SegmentIndex::SegmentIndex(SegmentIndex&&) noexcept = default;
SegmentIndex& SegmentIndex::operator=(SegmentIndex&&) noexcept = default;

void SegmentIndex::put(const std::string& key, uint32_t offset, uint32_t version) {
    bool is_new = !dht_.contains(key);
    dht_.addEntry(key, offset, version);
    if (is_new) {
        ++key_count_;
    }
}

bool SegmentIndex::get(const std::string& key,
                  std::vector<uint32_t>& offsets,
                  std::vector<uint32_t>& versions) const {
    return dht_.findAll(key, offsets, versions);
}

bool SegmentIndex::get(const std::string& key, uint64_t upper_bound,
                  uint64_t& offset, uint64_t& version) const {
    std::vector<uint32_t> offsets, versions;
    if (!dht_.findAll(key, offsets, versions)) return false;

    // Pairs are sorted desc by offset. Find first with version <= upper_bound.
    for (size_t i = 0; i < versions.size(); ++i) {
        if (versions[i] <= upper_bound) {
            offset = offsets[i];
            version = versions[i];
            return true;
        }
    }
    return false;
}

bool SegmentIndex::getLatest(const std::string& key,
                        uint32_t& offset, uint32_t& version) const {
    return dht_.findFirst(key, offset, version);
}

bool SegmentIndex::contains(const std::string& key) const {
    return dht_.contains(key);
}

Status SegmentIndex::writeTo(LogFile& file) {
    // Collect all entries via DHT-level forEach (has hash).
    std::vector<SegmentIndexEntry> entries;
    dht_.forEach([&entries](uint64_t hash, uint32_t offset, uint32_t version) {
        entries.push_back({hash, offset, version});
    });

    // Build the serialized buffer: header + entries + crc32.
    const size_t header_size = sizeof(SegmentIndexHeader);
    const size_t entries_size = entries.size() * sizeof(SegmentIndexEntry);
    const size_t payload_size = header_size + entries_size;
    const size_t total_size = payload_size + sizeof(uint32_t);

    std::vector<uint8_t> buf(total_size);

    // Write header.
    SegmentIndexHeader header;
    header.magic = kSegmentIndexMagic;
    header.entry_count = static_cast<uint32_t>(entries.size());
    header.key_count = static_cast<uint32_t>(key_count_);
    header.reserved = 0;
    std::memcpy(buf.data(), &header, header_size);

    // Write entries.
    if (!entries.empty()) {
        std::memcpy(buf.data() + header_size, entries.data(), entries_size);
    }

    // Compute and write CRC32 over header + entries.
    uint32_t checksum = crc32(buf.data(), payload_size);
    std::memcpy(buf.data() + payload_size, &checksum, sizeof(uint32_t));

    uint64_t write_offset;
    return file.append(buf.data(), total_size, write_offset);
}

Status SegmentIndex::readFrom(const LogFile& file, uint64_t offset) {
    // Read header.
    SegmentIndexHeader header;
    Status s = file.readAt(offset, &header, sizeof(header));
    if (!s.ok()) return s;

    if (header.magic != kSegmentIndexMagic) {
        return Status::Corruption("SegmentIndex: bad magic");
    }

    // Read entries.
    const size_t entries_size = header.entry_count * sizeof(SegmentIndexEntry);
    const size_t payload_size = sizeof(SegmentIndexHeader) + entries_size;
    const size_t total_size = payload_size + sizeof(uint32_t);

    std::vector<uint8_t> buf(total_size);
    s = file.readAt(offset, buf.data(), total_size);
    if (!s.ok()) return s;

    // Verify CRC32.
    uint32_t stored_crc;
    std::memcpy(&stored_crc, buf.data() + payload_size, sizeof(uint32_t));
    uint32_t computed_crc = crc32(buf.data(), payload_size);
    if (stored_crc != computed_crc) {
        return Status::ChecksumMismatch("SegmentIndex: checksum mismatch");
    }

    // Rebuild the index.
    clear();
    const SegmentIndexEntry* entries = reinterpret_cast<const SegmentIndexEntry*>(
        buf.data() + sizeof(SegmentIndexHeader));
    for (uint32_t i = 0; i < header.entry_count; ++i) {
        dht_.addEntryByHash(entries[i].hash, entries[i].offset, entries[i].version);
    }
    key_count_ = header.key_count;

    return Status::OK();
}

void SegmentIndex::forEach(const std::function<void(uint32_t offset, uint32_t version)>& fn) const {
    dht_.forEach([&fn](uint64_t /*hash*/, uint32_t offset, uint32_t version) {
        fn(offset, version);
    });
}

void SegmentIndex::forEachGroup(
    const std::function<void(uint64_t hash,
                             const std::vector<uint32_t>& offsets,
                             const std::vector<uint32_t>& versions)>& fn) const {
    dht_.forEachGroup(fn);
}

void SegmentIndex::setVisibleCount(size_t count) {
    visible_count_ = count;
}

size_t SegmentIndex::visibleCount() const {
    return visible_count_;
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
