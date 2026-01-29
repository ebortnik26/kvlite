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
    using namespace Payload;
    auto& pool = dht_.idVecPool();

    // If file_id exceeds inline capacity, go straight to overflow
    if (file_id > kMaxInlineFileId) {
        // Try insert with a temporary inline payload; if new, allocate overflow
        auto [payload, is_new] = dht_.insert(key, makeInline1(0));
        if (is_new) {
            uint32_t h = pool.allocate();
            pool.get(h).push_back(file_id);
            dht_.updatePayload(key, fromHandle(h));
            ++total_entries_;
            return;
        }
        // Existing key
        if (isInline(payload)) {
            uint32_t count = inlineCount(payload);
            uint32_t h = pool.allocate();
            auto& vec = pool.get(h);
            vec.push_back(file_id);
            vec.push_back(inlineFileId(payload, 0));
            if (count == 2) {
                vec.push_back(inlineFileId(payload, 1));
            }
            dht_.updatePayload(key, fromHandle(h));
            ++total_entries_;
        } else {
            auto& vec = pool.get(toHandle(payload));
            if (vec.empty() || vec[0] != file_id) {
                vec.insert(vec.begin(), file_id);
                ++total_entries_;
            }
        }
        return;
    }

    auto [payload, is_new] = dht_.insert(key, makeInline1(file_id));
    if (is_new) {
        ++total_entries_;
        return;
    }

    // Existing key
    if (isInline(payload)) {
        uint32_t count = inlineCount(payload);
        uint32_t fid0 = inlineFileId(payload, 0);
        if (fid0 == file_id) {
            return;  // already at front
        }
        if (count == 1) {
            dht_.updatePayload(key, makeInline2(file_id, fid0));
        } else {
            // count == 2, promote to overflow
            uint32_t fid1 = inlineFileId(payload, 1);
            if (fid1 == file_id) {
                // file_id exists but not at front: reorder to [file_id, fid0]
                dht_.updatePayload(key, makeInline2(file_id, fid0));
                return;  // no new entry, just reorder
            }
            uint32_t h = pool.allocate();
            pool.get(h) = {file_id, fid0, fid1};
            dht_.updatePayload(key, fromHandle(h));
        }
        ++total_entries_;
    } else {
        // overflow
        auto& vec = pool.get(toHandle(payload));
        if (vec.empty() || vec[0] != file_id) {
            vec.insert(vec.begin(), file_id);
            ++total_entries_;
        }
    }
}

bool L1Index::getFileIds(const std::string& key, std::vector<uint32_t>& out) const {
    using namespace Payload;

    auto [payload, found] = dht_.find(key);
    if (!found) return false;

    out.clear();
    if (isInline(payload)) {
        out.push_back(inlineFileId(payload, 0));
        if (inlineCount(payload) == 2) {
            out.push_back(inlineFileId(payload, 1));
        }
    } else {
        out = dht_.idVecPool().get(toHandle(payload));
    }
    return true;
}

bool L1Index::getLatest(const std::string& key, uint32_t& file_id) const {
    using namespace Payload;

    auto [payload, found] = dht_.find(key);
    if (!found) return false;

    if (isInline(payload)) {
        file_id = inlineFileId(payload, 0);
    } else {
        const auto& vec = dht_.idVecPool().get(toHandle(payload));
        if (vec.empty()) return false;
        file_id = vec[0];
    }
    return true;
}

bool L1Index::contains(const std::string& key) const {
    auto [payload, found] = dht_.find(key);
    return found;
}

void L1Index::remove(const std::string& key) {
    using namespace Payload;

    auto [payload, found] = dht_.find(key);
    if (!found) return;

    if (isInline(payload)) {
        total_entries_ -= inlineCount(payload);
    } else {
        uint32_t h = toHandle(payload);
        total_entries_ -= dht_.idVecPool().get(h).size();
        dht_.idVecPool().release(h);
    }
    dht_.remove(key);
}

void L1Index::removeFile(const std::string& key, uint32_t file_id) {
    using namespace Payload;
    auto& pool = dht_.idVecPool();

    auto [payload, found] = dht_.find(key);
    if (!found) return;

    if (isInline(payload)) {
        uint32_t count = inlineCount(payload);
        uint32_t fid0 = inlineFileId(payload, 0);
        if (count == 1) {
            if (fid0 == file_id) {
                dht_.remove(key);
                --total_entries_;
            }
        } else {
            // count == 2
            uint32_t fid1 = inlineFileId(payload, 1);
            if (fid0 == file_id) {
                dht_.updatePayload(key, makeInline1(fid1));
                --total_entries_;
            } else if (fid1 == file_id) {
                dht_.updatePayload(key, makeInline1(fid0));
                --total_entries_;
            }
        }
    } else {
        // overflow
        uint32_t h = toHandle(payload);
        auto& vec = pool.get(h);
        auto it = std::find(vec.begin(), vec.end(), file_id);
        if (it != vec.end()) {
            vec.erase(it);
            --total_entries_;
        }
        if (vec.empty()) {
            pool.release(h);
            dht_.remove(key);
        } else if (vec.size() <= 2) {
            // Demote back to inline if all remaining ids fit
            bool can_inline = true;
            for (uint32_t fid : vec) {
                if (fid > kMaxInlineFileId) {
                    can_inline = false;
                    break;
                }
            }
            if (can_inline) {
                uint64_t new_payload;
                if (vec.size() == 1) {
                    new_payload = makeInline1(vec[0]);
                } else {
                    new_payload = makeInline2(vec[0], vec[1]);
                }
                pool.release(h);
                dht_.updatePayload(key, new_payload);
            }
        }
    }
}

void L1Index::forEach(
    const std::function<void(const std::vector<uint32_t>&)>& fn) const {
    using namespace Payload;
    const auto& pool = dht_.idVecPool();

    dht_.forEach([&fn, &pool](uint64_t /*hash*/, uint64_t payload) {
        if (isInline(payload)) {
            std::vector<uint32_t> fids;
            fids.push_back(inlineFileId(payload, 0));
            if (inlineCount(payload) == 2) {
                fids.push_back(inlineFileId(payload, 1));
            }
            fn(fids);
        } else {
            fn(pool.get(toHandle(payload)));
        }
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
static constexpr uint32_t kVersion = 3;

Status L1Index::saveSnapshot(const std::string& path) const {
    using namespace Payload;

    std::ofstream file(path, std::ios::binary);
    if (!file) {
        return Status::IOError("Failed to create snapshot file: " + path);
    }

    CRC32Writer writer(file);

    // Header
    writer.write(kMagic, 4);
    writer.writeVal(kVersion);

    // Number of records
    uint64_t num_records = dht_.size();
    writer.writeVal(num_records);

    // For each record: decode payload into file_ids and write
    const auto& pool = dht_.idVecPool();
    dht_.forEach([&writer, &pool](uint64_t hash, uint64_t payload) {
        writer.writeVal(hash);

        if (isInline(payload)) {
            uint32_t count = inlineCount(payload);
            writer.writeVal(count);
            for (uint32_t i = 0; i < count; ++i) {
                uint32_t fid = inlineFileId(payload, i);
                writer.writeVal(fid);
            }
        } else {
            const auto& file_ids = pool.get(toHandle(payload));
            uint32_t num_file_ids = static_cast<uint32_t>(file_ids.size());
            writer.writeVal(num_file_ids);
            for (uint32_t fid : file_ids) {
                writer.writeVal(fid);
            }
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
    using namespace Payload;

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

        std::vector<uint32_t> file_ids(num_file_ids);
        for (uint32_t e = 0; e < num_file_ids; ++e) {
            if (!reader.readVal(file_ids[e])) {
                return Status::Corruption("Failed to read file_id");
            }
        }

        // Build payload: inline if <=2 file_ids and all fit in 31 bits, else overflow
        uint64_t payload;
        bool can_inline = (num_file_ids <= 2);
        if (can_inline) {
            for (uint32_t fid : file_ids) {
                if (fid > kMaxInlineFileId) {
                    can_inline = false;
                    break;
                }
            }
        }

        if (can_inline && num_file_ids == 1) {
            payload = makeInline1(file_ids[0]);
        } else if (can_inline && num_file_ids == 2) {
            payload = makeInline2(file_ids[0], file_ids[1]);
        } else {
            uint32_t h = dht_.idVecPool().allocate();
            dht_.idVecPool().get(h) = std::move(file_ids);
            payload = fromHandle(h);
        }

        dht_.insertByHash(hash, payload);
        total_entries_ += num_file_ids;
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
