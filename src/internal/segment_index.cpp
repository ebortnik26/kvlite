#include "internal/segment_index.h"

#include <cstring>
#include <vector>

#include "internal/crc32.h"
#include "internal/delta_hash_table.h"
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
    uint64_t packed_version;
};

static ReadOnlyDeltaHashTable::Config defaultDHTConfig() {
    ReadOnlyDeltaHashTable::Config config;
    config.bucket_bits = 16;
    config.lslot_bits = 4;
    config.bucket_bytes = 256;
    return config;
}

SegmentIndex::SegmentIndex() : dht_(defaultDHTConfig()) {}

SegmentIndex::~SegmentIndex() = default;
SegmentIndex::SegmentIndex(SegmentIndex&&) noexcept = default;
SegmentIndex& SegmentIndex::operator=(SegmentIndex&&) noexcept = default;

void SegmentIndex::put(std::string_view key, uint32_t offset, uint64_t packed_version) {
    uint64_t h = dhtHashBytes(key.data(), key.size());
    if (dht_.addEntryIsNew(h, packed_version, offset)) {
        ++key_count_;
    }
}

bool SegmentIndex::get(const std::string& key,
                  std::vector<uint32_t>& offsets,
                  std::vector<uint64_t>& packed_versions) const {
    // DHT stores (packed_version, id=offset)
    std::vector<uint64_t> pvs;
    std::vector<uint32_t> ids;
    uint64_t h = dhtHashBytes(key.data(), key.size());
    if (!dht_.findAll(h, pvs, ids)) return false;
    packed_versions = std::move(pvs);
    offsets = std::move(ids);
    return true;
}

bool SegmentIndex::get(const std::string& key, uint64_t upper_bound,
                  uint64_t& offset, uint64_t& packed_version) const {
    std::vector<uint32_t> offsets;
    std::vector<uint64_t> packed_versions;
    if (!get(key, offsets, packed_versions)) return false;

    // Pairs are sorted desc by packed_version. Find first with packed_version <= upper_bound.
    for (size_t i = 0; i < packed_versions.size(); ++i) {
        if (packed_versions[i] <= upper_bound) {
            offset = offsets[i];
            packed_version = packed_versions[i];
            return true;
        }
    }
    return false;
}

bool SegmentIndex::getLatest(const std::string& key,
                        uint32_t& offset, uint64_t& packed_version) const {
    // DHT stores (packed_version, id=offset)
    uint64_t pv;
    uint32_t id;
    uint64_t h = dhtHashBytes(key.data(), key.size());
    if (!dht_.findFirst(h, pv, id)) return false;
    packed_version = pv;
    offset = id;
    return true;
}

bool SegmentIndex::contains(const std::string& key) const {
    uint64_t h = dhtHashBytes(key.data(), key.size());
    return dht_.contains(h);
}

Status SegmentIndex::writeTo(LogFile& file) {
    // Seal the DHT — no more writes after serialization.
    dht_.seal();

    // Single-buffer serialization: header + entries + crc32.
    // No intermediate vector — forEach writes directly into the output buffer.
    const size_t num_entries = dht_.size();
    const size_t header_size = sizeof(SegmentIndexHeader);
    const size_t entries_size = num_entries * sizeof(SegmentIndexEntry);
    const size_t payload_size = header_size + entries_size;
    const size_t total_size = payload_size + sizeof(uint32_t);

    std::vector<uint8_t> buf(total_size);

    // Write header.
    SegmentIndexHeader header;
    header.magic = kSegmentIndexMagic;
    header.entry_count = static_cast<uint32_t>(num_entries);
    header.key_count = static_cast<uint32_t>(key_count_);
    header.reserved = 0;
    std::memcpy(buf.data(), &header, header_size);

    // Write entries directly into buffer.
    size_t entry_offset = header_size;
    dht_.forEach([&buf, &entry_offset](uint64_t hash, uint64_t packed_version, uint32_t id) {
        SegmentIndexEntry entry{hash, id, packed_version};
        std::memcpy(buf.data() + entry_offset, &entry, sizeof(entry));
        entry_offset += sizeof(entry);
    });

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
        // DHT stores (packed_version, id=offset)
        dht_.addEntry(entries[i].hash, entries[i].packed_version, entries[i].offset);
    }
    key_count_ = header.key_count;

    // Seal the DHT — deserialized indexes are immutable.
    dht_.seal();

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
    // DHT stores (packed_versions, ids=offsets) — swap for caller
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
