#include "internal/global_index.h"

#include <algorithm>
#include <cstring>
#include <fstream>

#include "internal/global_index_wal.h"

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

GlobalIndex::GlobalIndex() = default;

GlobalIndex::~GlobalIndex() {
    if (is_open_) {
        close();
    }
}

// --- Lifecycle ---

Status GlobalIndex::open(const std::string& db_path, const Options& options) {
    if (is_open_) {
        return Status::InvalidArgument("Already open");
    }
    db_path_ = db_path;
    options_ = options;
    is_open_ = true;
    return Status::OK();
}

Status GlobalIndex::recover() {
    std::string path = snapshotPath();
    Status s = loadSnapshot(path);
    if (s.ok()) {
        return Status::OK();
    }
    // No snapshot or corrupted — start with empty index.
    // Caller (DB::open) will rebuild from segments if needed.
    return Status::OK();
}

Status GlobalIndex::close() {
    if (!is_open_) {
        return Status::OK();
    }
    // Persist index to snapshot before closing.
    Status s = saveSnapshot(snapshotPath());
    if (!s.ok()) {
        wal_.reset();
        is_open_ = false;
        return s;
    }
    wal_.reset();
    is_open_ = false;
    return Status::OK();
}

bool GlobalIndex::isOpen() const {
    return is_open_;
}

// --- Index Operations ---

Status GlobalIndex::put(const std::string& key, uint64_t version, uint32_t segment_id) {
    if (!dht_.contains(key)) {
        ++key_count_;
    }
    // DHT "offsets" field = version, "versions" field = segment_id
    dht_.addEntry(key, static_cast<uint32_t>(version), segment_id);
    updates_since_snapshot_++;
    return Status::OK();
}

bool GlobalIndex::get(const std::string& key,
                  std::vector<uint32_t>& segment_ids,
                  std::vector<uint64_t>& versions) const {
    std::vector<uint32_t> raw_vers;   // DHT "offsets" = versions
    std::vector<uint32_t> raw_segs;   // DHT "versions" = segment_ids
    if (!dht_.findAll(key, raw_vers, raw_segs)) return false;
    segment_ids.clear();
    versions.clear();
    segment_ids.reserve(raw_segs.size());
    versions.reserve(raw_vers.size());
    for (size_t i = 0; i < raw_vers.size(); ++i) {
        versions.push_back(static_cast<uint64_t>(raw_vers[i]));
        segment_ids.push_back(raw_segs[i]);
    }
    return true;
}

bool GlobalIndex::get(const std::string& key, uint64_t upper_bound,
                  uint64_t& version, uint32_t& segment_id) const {
    std::vector<uint32_t> raw_vers;
    std::vector<uint32_t> raw_segs;
    if (!dht_.findAll(key, raw_vers, raw_segs)) return false;
    // Entries are sorted by version descending; find the first <= upper_bound.
    for (size_t i = 0; i < raw_vers.size(); ++i) {
        if (static_cast<uint64_t>(raw_vers[i]) <= upper_bound) {
            version = static_cast<uint64_t>(raw_vers[i]);
            segment_id = raw_segs[i];
            return true;
        }
    }
    return false;
}

Status GlobalIndex::getLatest(const std::string& key,
                        uint64_t& version, uint32_t& segment_id) const {
    uint32_t raw_ver, raw_seg;
    if (!dht_.findFirst(key, raw_ver, raw_seg)) {
        return Status::NotFound(key);
    }
    version = static_cast<uint64_t>(raw_ver);
    segment_id = raw_seg;
    return Status::OK();
}

bool GlobalIndex::contains(const std::string& key) const {
    return dht_.contains(key);
}

Status GlobalIndex::remove(const std::string& key) {
    size_t removed = dht_.removeAll(key);
    if (removed > 0) {
        --key_count_;
    }
    updates_since_snapshot_++;
    return Status::OK();
}

void GlobalIndex::removeSegment(const std::string& key, uint32_t segment_id) {
    // removeBySecond removes entries where DHT "versions" field = segment_id
    size_t removed = dht_.removeBySecond(key, segment_id);
    if (removed > 0 && !dht_.contains(key)) {
        --key_count_;
    }
}

// --- Iteration ---

void GlobalIndex::forEachGroup(
    const std::function<void(uint64_t hash,
                             const std::vector<uint32_t>& versions,
                             const std::vector<uint32_t>& segment_ids)>& fn) const {
    // DHT "offsets" = versions, DHT "versions" = segment_ids
    dht_.forEachGroup([&fn](uint64_t hash,
                            const std::vector<uint32_t>& offsets,
                            const std::vector<uint32_t>& versions) {
        fn(hash, offsets, versions);
    });
}

void GlobalIndex::clear() {
    dht_.clear();
    key_count_ = 0;
}

// --- Maintenance ---

Status GlobalIndex::snapshot() {
    // Stub: no auto-snapshot yet.
    updates_since_snapshot_ = 0;
    return Status::OK();
}

Status GlobalIndex::sync() {
    // Stub: no WAL yet.
    return Status::OK();
}

// --- Statistics ---

size_t GlobalIndex::keyCount() const {
    return key_count_;
}

size_t GlobalIndex::entryCount() const {
    return dht_.size();
}

size_t GlobalIndex::memoryUsage() const {
    return dht_.memoryUsage();
}

uint64_t GlobalIndex::updatesSinceSnapshot() const {
    return updates_since_snapshot_;
}

// --- Persistence ---

static constexpr char kMagic[4] = {'L', '1', 'I', 'X'};
static constexpr uint32_t kVersion = 6;

Status GlobalIndex::saveSnapshot(const std::string& path) const {
    std::ofstream file(path, std::ios::binary);
    if (!file) {
        return Status::IOError("Failed to create snapshot file: " + path);
    }

    CRC32Writer writer(file);

    // Header
    writer.write(kMagic, 4);
    writer.writeVal(kVersion);

    uint64_t num_entries = dht_.size();
    writer.writeVal(num_entries);

    uint64_t key_count = key_count_;
    writer.writeVal(key_count);

    // Per entry: [hash:8][version:4][segment_id:4]
    dht_.forEach([&writer](uint64_t hash, uint32_t version, uint32_t segment_id) {
        writer.writeVal(hash);
        writer.writeVal(version);
        writer.writeVal(segment_id);
    });

    // Checksum
    uint32_t checksum = writer.finalize();
    file.write(reinterpret_cast<const char*>(&checksum), sizeof(checksum));

    if (!file.good()) {
        return Status::IOError("Failed to write snapshot file: " + path);
    }

    return Status::OK();
}

Status GlobalIndex::loadSnapshot(const std::string& path) {
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

    // Number of entries
    uint64_t num_entries;
    if (!reader.readVal(num_entries)) {
        return Status::Corruption("Failed to read entry count");
    }

    // Key count
    uint64_t key_count;
    if (!reader.readVal(key_count)) {
        return Status::Corruption("Failed to read key count");
    }

    // Clear current state and rebuild
    clear();

    for (uint64_t i = 0; i < num_entries; ++i) {
        uint64_t hash;
        uint32_t ver, seg;
        if (!reader.readVal(hash) || !reader.readVal(ver) || !reader.readVal(seg)) {
            return Status::Corruption("Failed to read entry");
        }
        dht_.addEntryByHash(hash, ver, seg);
    }

    key_count_ = static_cast<size_t>(key_count);

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

// --- Private ---

Status GlobalIndex::maybeSnapshot() {
    // Stub: no auto-snapshot yet.
    return Status::OK();
}

std::string GlobalIndex::snapshotPath() const {
    return db_path_ + "/global_index.snapshot";
}

std::string GlobalIndex::walPath() const {
    return db_path_ + "/global_index.wal";
}

}  // namespace internal
}  // namespace kvlite
