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
    // If already the latest, no-op
    uint32_t current;
    if (dht_.findFirst(key, current) && current == file_id) {
        return;
    }

    bool is_new = !dht_.contains(key);
    dht_.addEntry(key, file_id);
    if (is_new) {
        ++key_count_;
    }
}

bool L1Index::getFileIds(const std::string& key, std::vector<uint32_t>& out) const {
    return dht_.findAll(key, out);
}

bool L1Index::getLatest(const std::string& key, uint32_t& file_id) const {
    return dht_.findFirst(key, file_id);
}

bool L1Index::contains(const std::string& key) const {
    return dht_.contains(key);
}

void L1Index::remove(const std::string& key) {
    size_t removed = dht_.removeAll(key);
    if (removed > 0) {
        --key_count_;
    }
}

void L1Index::removeFile(const std::string& key, uint32_t file_id) {
    if (dht_.removeEntry(key, file_id)) {
        // Check if key is now gone
        if (!dht_.contains(key)) {
            --key_count_;
        }
    }
}

void L1Index::forEach(
    const std::function<void(const std::vector<uint32_t>&)>& fn) const {
    dht_.forEachGroup([&fn](uint64_t /*hash*/, const std::vector<uint32_t>& values) {
        fn(values);
    });
}

size_t L1Index::keyCount() const {
    return key_count_;
}

size_t L1Index::entryCount() const {
    return dht_.size();
}

size_t L1Index::memoryUsage() const {
    return dht_.memoryUsage();
}

void L1Index::clear() {
    dht_.clear();
    key_count_ = 0;
}

// --- Persistence ---

static constexpr char kMagic[4] = {'L', '1', 'I', 'X'};
static constexpr uint32_t kVersion = 4;

Status L1Index::saveSnapshot(const std::string& path) const {
    std::ofstream file(path, std::ios::binary);
    if (!file) {
        return Status::IOError("Failed to create snapshot file: " + path);
    }

    CRC32Writer writer(file);

    // Header
    writer.write(kMagic, 4);
    writer.writeVal(kVersion);

    // Count groups (unique fingerprints = keys)
    uint64_t num_records = key_count_;
    writer.writeVal(num_records);

    // Write each group
    dht_.forEachGroup([&writer](uint64_t hash, const std::vector<uint32_t>& values) {
        writer.writeVal(hash);
        uint32_t num_file_ids = static_cast<uint32_t>(values.size());
        writer.writeVal(num_file_ids);
        for (uint32_t fid : values) {
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

    // Version — accept both v3 and v4 (same format)
    uint32_t version;
    if (!reader.readVal(version) || (version != 3 && version != kVersion)) {
        return Status::Corruption("Unsupported snapshot version");
    }

    // Number of records
    uint64_t num_records;
    if (!reader.readVal(num_records)) {
        return Status::Corruption("Failed to read record count");
    }

    // Clear current state and rebuild
    clear();

    for (uint64_t k = 0; k < num_records; ++k) {
        uint64_t hash;
        if (!reader.readVal(hash)) {
            return Status::Corruption("Failed to read hash");
        }

        uint32_t num_file_ids;
        if (!reader.readVal(num_file_ids)) {
            return Status::Corruption("Failed to read file_id count");
        }

        for (uint32_t e = 0; e < num_file_ids; ++e) {
            uint32_t fid;
            if (!reader.readVal(fid)) {
                return Status::Corruption("Failed to read file_id");
            }
            dht_.addEntryByHash(hash, fid);
        }

        ++key_count_;
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
