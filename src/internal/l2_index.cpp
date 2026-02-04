#include "internal/l2_index.h"

#include <cstring>
#include <vector>

#include "internal/crc32.h"
#include "internal/log_file.h"

namespace kvlite {
namespace internal {

static const uint32_t kL2IndexMagic = 0x4C324958;  // "L2IX"

struct L2IndexHeader {
    uint32_t magic;
    uint32_t entry_count;
    uint32_t key_count;
    uint32_t reserved;
};

struct L2IndexEntry {
    uint64_t hash;
    uint32_t offset;
    uint32_t version;
};

static L2DeltaHashTable::Config defaultDHTConfig() {
    L2DeltaHashTable::Config config;
    config.bucket_bits = 16;
    config.lslot_bits = 4;
    config.bucket_bytes = 256;
    return config;
}

L2Index::L2Index() : dht_(defaultDHTConfig()) {}

L2Index::~L2Index() = default;
L2Index::L2Index(L2Index&&) noexcept = default;
L2Index& L2Index::operator=(L2Index&&) noexcept = default;

void L2Index::put(const std::string& key, uint32_t offset, uint32_t version) {
    bool is_new = !dht_.contains(key);
    dht_.addEntry(key, offset, version);
    if (is_new) {
        ++key_count_;
    }
}

bool L2Index::get(const std::string& key,
                  std::vector<uint32_t>& offsets,
                  std::vector<uint32_t>& versions) const {
    return dht_.findAll(key, offsets, versions);
}

bool L2Index::get(const std::string& key, uint64_t upper_bound,
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

bool L2Index::getLatest(const std::string& key,
                        uint32_t& offset, uint32_t& version) const {
    return dht_.findFirst(key, offset, version);
}

bool L2Index::contains(const std::string& key) const {
    return dht_.contains(key);
}

Status L2Index::writeTo(LogFile& file) {
    // Collect all entries via DHT-level forEach (has hash).
    std::vector<L2IndexEntry> entries;
    dht_.forEach([&entries](uint64_t hash, uint32_t offset, uint32_t version) {
        entries.push_back({hash, offset, version});
    });

    // Build the serialized buffer: header + entries + crc32.
    const size_t header_size = sizeof(L2IndexHeader);
    const size_t entries_size = entries.size() * sizeof(L2IndexEntry);
    const size_t payload_size = header_size + entries_size;
    const size_t total_size = payload_size + sizeof(uint32_t);

    std::vector<uint8_t> buf(total_size);

    // Write header.
    L2IndexHeader header;
    header.magic = kL2IndexMagic;
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

Status L2Index::readFrom(LogFile& file, uint64_t offset) {
    // Read header.
    L2IndexHeader header;
    Status s = file.readAt(offset, &header, sizeof(header));
    if (!s.ok()) return s;

    if (header.magic != kL2IndexMagic) {
        return Status::Corruption("L2Index: bad magic");
    }

    // Read entries.
    const size_t entries_size = header.entry_count * sizeof(L2IndexEntry);
    const size_t payload_size = sizeof(L2IndexHeader) + entries_size;
    const size_t total_size = payload_size + sizeof(uint32_t);

    std::vector<uint8_t> buf(total_size);
    s = file.readAt(offset, buf.data(), total_size);
    if (!s.ok()) return s;

    // Verify CRC32.
    uint32_t stored_crc;
    std::memcpy(&stored_crc, buf.data() + payload_size, sizeof(uint32_t));
    uint32_t computed_crc = crc32(buf.data(), payload_size);
    if (stored_crc != computed_crc) {
        return Status::ChecksumMismatch("L2Index: checksum mismatch");
    }

    // Rebuild the index.
    clear();
    const L2IndexEntry* entries = reinterpret_cast<const L2IndexEntry*>(
        buf.data() + sizeof(L2IndexHeader));
    for (uint32_t i = 0; i < header.entry_count; ++i) {
        dht_.addEntryByHash(entries[i].hash, entries[i].offset, entries[i].version);
    }
    key_count_ = header.key_count;

    return Status::OK();
}

size_t L2Index::keyCount() const {
    return key_count_;
}

size_t L2Index::entryCount() const {
    return dht_.size();
}

size_t L2Index::memoryUsage() const {
    return dht_.memoryUsage();
}

void L2Index::clear() {
    dht_.clear();
    key_count_ = 0;
}

}  // namespace internal
}  // namespace kvlite
