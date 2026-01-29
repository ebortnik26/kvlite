#include "internal/l1_index.h"

#include <algorithm>
#include <cstring>
#include <fstream>

namespace kvlite {
namespace internal {

// Simple CRC32 for snapshot checksumming (same polynomial as zlib).
static uint32_t crc32(const void* data, size_t len) {
    const uint8_t* buf = static_cast<const uint8_t*>(data);
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < len; ++i) {
        crc ^= buf[i];
        for (int j = 0; j < 8; ++j) {
            crc = (crc >> 1) ^ (0xEDB88320 & (-(crc & 1)));
        }
    }
    return ~crc;
}

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

// Use smaller DHT config to avoid allocating 1M buckets by default.
// For typical workloads (<100K keys), 16-bit bucket index is sufficient.
static DeltaHashTable::Config defaultDHTConfig() {
    DeltaHashTable::Config config;
    config.bucket_bits = 16;    // 64K buckets
    config.lslot_bits = 4;      // 16 lslots per bucket
    config.bucket_bytes = 256;  // smaller buckets
    return config;
}

L1Index::L1Index() : dht_(defaultDHTConfig()) {}

L1Index::~L1Index() = default;

void L1Index::put(const std::string& key, uint32_t file_id) {
    KeyRecord* record = dht_.insert(key);
    if (record->file_ids.empty() || record->file_ids[0] != file_id) {
        record->file_ids.insert(record->file_ids.begin(), file_id);
        ++total_entries_;
    }
}

const std::vector<uint32_t>* L1Index::getFileIds(const std::string& key) const {
    KeyRecord* record = dht_.find(key);
    if (!record || record->file_ids.empty()) {
        return nullptr;
    }
    return &record->file_ids;
}

bool L1Index::getLatest(const std::string& key, uint32_t& file_id) const {
    KeyRecord* record = dht_.find(key);
    if (!record || record->file_ids.empty()) {
        return false;
    }
    file_id = record->file_ids[0];
    return true;
}

bool L1Index::contains(const std::string& key) const {
    KeyRecord* record = dht_.find(key);
    return record != nullptr && !record->file_ids.empty();
}

void L1Index::remove(const std::string& key) {
    KeyRecord* record = dht_.find(key);
    if (record) {
        total_entries_ -= record->file_ids.size();
        dht_.remove(key);
    }
}

void L1Index::removeFile(const std::string& key, uint32_t file_id) {
    KeyRecord* record = dht_.find(key);
    if (!record) return;

    auto it = std::find(record->file_ids.begin(), record->file_ids.end(), file_id);
    if (it != record->file_ids.end()) {
        record->file_ids.erase(it);
        --total_entries_;
    }

    if (record->file_ids.empty()) {
        dht_.remove(key);
    }
}

void L1Index::forEach(
    const std::function<void(const std::string&,
                              const std::vector<uint32_t>&)>& fn) const {
    dht_.forEach([&fn](const KeyRecord& record) {
        fn(record.key, record.file_ids);
    });
}

void L1Index::forEachKey(
    const std::function<void(const std::string&)>& fn) const {
    dht_.forEach([&fn](const KeyRecord& record) {
        fn(record.key);
    });
}

size_t L1Index::keyCount() const {
    return dht_.size();
}

size_t L1Index::entryCount() const {
    return total_entries_;
}

size_t L1Index::memoryUsage() const {
    return dht_.memoryUsage();
}

void L1Index::clear() {
    dht_.clear();
    total_entries_ = 0;
}

// --- Persistence ---

static constexpr char kMagic[4] = {'L', '1', 'I', 'X'};
static constexpr uint32_t kVersion = 2;

Status L1Index::saveSnapshot(const std::string& path) const {
    std::ofstream file(path, std::ios::binary);
    if (!file) {
        return Status::IOError("Failed to create snapshot file: " + path);
    }

    CRC32Writer writer(file);

    // Header
    writer.write(kMagic, 4);
    writer.writeVal(kVersion);

    // Number of keys
    uint64_t num_keys = dht_.size();
    writer.writeVal(num_keys);

    // For each key
    dht_.forEach([&writer](const KeyRecord& record) {
        uint32_t key_len = static_cast<uint32_t>(record.key.size());
        writer.writeVal(key_len);
        writer.write(record.key.data(), key_len);

        uint32_t num_file_ids = static_cast<uint32_t>(record.file_ids.size());
        writer.writeVal(num_file_ids);

        for (uint32_t fid : record.file_ids) {
            writer.writeVal(fid);
        }
    });

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
            return Status::Corruption("Failed to read key data");
        }

        uint32_t num_file_ids;
        if (!reader.readVal(num_file_ids)) {
            return Status::Corruption("Failed to read file_id count");
        }

        KeyRecord* record = dht_.insert(key);
        record->file_ids.reserve(num_file_ids);
        for (uint32_t e = 0; e < num_file_ids; ++e) {
            uint32_t file_id;
            if (!reader.readVal(file_id)) {
                return Status::Corruption("Failed to read file_id");
            }
            record->file_ids.push_back(file_id);
            ++total_entries_;
        }
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
