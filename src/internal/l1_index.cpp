#include "internal/l1_index.h"

#include <algorithm>
#include <cstring>
#include <fstream>

namespace kvlite {
namespace internal {

// Helper: accumulate CRC over multiple writes
struct CRC32Writer {
    std::ofstream& out;
    uint32_t crc = 0xFFFFFFFF;

    explicit CRC32Writer(std::ofstream& o) : out(o) {}

    bool write(const void* data, size_t len) {
        const uint8_t* buf = static_cast<const uint8_t*>(data);
        for (size_t i = 0; i < len; ++i) {
            crc ^= buf[i];
            for (int j = 0; j < 8; ++j) {
                crc = (crc >> 1) ^ (0xEDB88320 & (-(crc & 1)));
            }
        }
        out.write(reinterpret_cast<const char*>(data), len);
        return out.good();
    }

    template<typename T>
    bool writeVal(const T& val) {
        return write(&val, sizeof(val));
    }

    uint32_t finalize() const { return ~crc; }
};

struct CRC32Reader {
    std::ifstream& in;
    uint32_t crc = 0xFFFFFFFF;

    explicit CRC32Reader(std::ifstream& i) : in(i) {}

    bool read(void* data, size_t len) {
        in.read(reinterpret_cast<char*>(data), len);
        if (!in.good()) return false;
        const uint8_t* buf = static_cast<const uint8_t*>(data);
        for (size_t i = 0; i < len; ++i) {
            crc ^= buf[i];
            for (int j = 0; j < 8; ++j) {
                crc = (crc >> 1) ^ (0xEDB88320 & (-(crc & 1)));
            }
        }
        return true;
    }

    template<typename T>
    bool readVal(T& val) {
        return read(&val, sizeof(val));
    }

    uint32_t finalize() const { return ~crc; }
};

L1Index::L1Index() = default;

L1Index::~L1Index() = default;

void L1Index::put(const std::string& key, uint64_t version, uint32_t segment_id) {
    auto& entries = index_[key];
    if (entries.empty()) {
        ++key_count_;
    }
    // Insert maintaining version descending order.
    auto pos = std::lower_bound(entries.begin(), entries.end(), version,
        [](const Entry& e, uint64_t v) { return e.version > v; });
    entries.insert(pos, {segment_id, version});
    ++entry_count_;
}

bool L1Index::get(const std::string& key,
                  std::vector<uint32_t>& segment_ids,
                  std::vector<uint64_t>& versions) const {
    auto it = index_.find(key);
    if (it == index_.end() || it->second.empty()) return false;
    segment_ids.clear();
    versions.clear();
    segment_ids.reserve(it->second.size());
    versions.reserve(it->second.size());
    for (const auto& e : it->second) {
        segment_ids.push_back(e.segment_id);
        versions.push_back(e.version);
    }
    return true;
}

bool L1Index::get(const std::string& key, uint64_t upper_bound,
                  uint64_t& version, uint32_t& segment_id) const {
    auto it = index_.find(key);
    if (it == index_.end()) return false;
    // Entries are sorted by version descending; find the first <= upper_bound.
    for (const auto& e : it->second) {
        if (e.version <= upper_bound) {
            segment_id = e.segment_id;
            version = e.version;
            return true;
        }
    }
    return false;
}

bool L1Index::getLatest(const std::string& key,
                        uint64_t& version, uint32_t& segment_id) const {
    auto it = index_.find(key);
    if (it == index_.end() || it->second.empty()) return false;
    version = it->second.front().version;
    segment_id = it->second.front().segment_id;
    return true;
}

bool L1Index::contains(const std::string& key) const {
    auto it = index_.find(key);
    return it != index_.end() && !it->second.empty();
}

void L1Index::remove(const std::string& key) {
    auto it = index_.find(key);
    if (it != index_.end() && !it->second.empty()) {
        entry_count_ -= it->second.size();
        it->second.clear();
        index_.erase(it);
        --key_count_;
    }
}

void L1Index::removeSegment(const std::string& key, uint32_t segment_id) {
    auto it = index_.find(key);
    if (it == index_.end()) return;
    auto& entries = it->second;
    size_t old_size = entries.size();
    entries.erase(
        std::remove_if(entries.begin(), entries.end(),
                        [segment_id](const Entry& e) {
                            return e.segment_id == segment_id;
                        }),
        entries.end());
    entry_count_ -= (old_size - entries.size());
    if (entries.empty()) {
        index_.erase(it);
        --key_count_;
    }
}

size_t L1Index::keyCount() const {
    return key_count_;
}

size_t L1Index::entryCount() const {
    return entry_count_;
}

size_t L1Index::memoryUsage() const {
    size_t usage = sizeof(*this);
    for (const auto& kv : index_) {
        usage += kv.first.capacity() + sizeof(kv.first);
        usage += kv.second.capacity() * sizeof(Entry) + sizeof(kv.second);
    }
    return usage;
}

void L1Index::clear() {
    index_.clear();
    key_count_ = 0;
    entry_count_ = 0;
}

// --- Persistence ---

static constexpr char kMagic[4] = {'L', '1', 'I', 'X'};
static constexpr uint32_t kVersion = 5;

Status L1Index::saveSnapshot(const std::string& path) const {
    std::ofstream file(path, std::ios::binary);
    if (!file) {
        return Status::IOError("Failed to create snapshot file: " + path);
    }

    CRC32Writer writer(file);

    // Header
    writer.write(kMagic, 4);
    writer.writeVal(kVersion);

    uint64_t num_keys = key_count_;
    writer.writeVal(num_keys);

    for (const auto& kv : index_) {
        uint32_t key_len = static_cast<uint32_t>(kv.first.size());
        writer.writeVal(key_len);
        writer.write(kv.first.data(), key_len);
        uint32_t num_entries = static_cast<uint32_t>(kv.second.size());
        writer.writeVal(num_entries);
        for (const auto& e : kv.second) {
            writer.writeVal(e.segment_id);
            uint64_t ver = e.version;
            writer.writeVal(ver);
        }
    }

    // Checksum
    uint32_t checksum = writer.finalize();
    file.write(reinterpret_cast<const char*>(&checksum), sizeof(checksum));

    if (!file.good()) {
        return Status::IOError("Failed to write snapshot file: " + path);
    }

    return Status::OK();
}

Status L1Index::loadSnapshot(const std::string& path) {
    std::ifstream file(path, std::ios::binary);
    if (!file) {
        return Status::IOError("Failed to open snapshot file: " + path);
    }

    CRC32Reader reader(file);

    // Magic
    char magic[4];
    if (!reader.read(magic, 4) || std::memcmp(magic, kMagic, 4) != 0) {
        return Status::Corruption("Invalid snapshot magic");
    }

    // Version
    uint32_t version;
    if (!reader.readVal(version) || version != kVersion) {
        return Status::Corruption("Unsupported snapshot version");
    }

    // Number of keys
    uint64_t num_keys;
    if (!reader.readVal(num_keys)) {
        return Status::Corruption("Failed to read key count");
    }

    // Clear current state and rebuild
    clear();

    for (uint64_t k = 0; k < num_keys; ++k) {
        uint32_t key_len;
        if (!reader.readVal(key_len)) {
            return Status::Corruption("Failed to read key length");
        }

        std::string key(key_len, '\0');
        if (!reader.read(key.data(), key_len)) {
            return Status::Corruption("Failed to read key");
        }

        uint32_t num_entries;
        if (!reader.readVal(num_entries)) {
            return Status::Corruption("Failed to read entry count");
        }

        auto& entries = index_[key];
        entries.reserve(num_entries);
        for (uint32_t e = 0; e < num_entries; ++e) {
            uint32_t segment_id;
            uint64_t ver;
            if (!reader.readVal(segment_id) || !reader.readVal(ver)) {
                return Status::Corruption("Failed to read entry");
            }
            entries.push_back({segment_id, ver});
        }

        ++key_count_;
        entry_count_ += num_entries;
    }

    // Verify checksum
    uint32_t expected_crc = reader.finalize();
    uint32_t stored_crc;
    file.read(reinterpret_cast<char*>(&stored_crc), sizeof(stored_crc));
    if (!file.good()) {
        return Status::Corruption("Failed to read checksum");
    }
    if (stored_crc != expected_crc) {
        return Status::ChecksumMismatch("Snapshot checksum mismatch");
    }

    return Status::OK();
}

}  // namespace internal
}  // namespace kvlite
