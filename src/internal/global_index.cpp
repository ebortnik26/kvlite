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

GlobalIndex::GlobalIndex()
    : wal_(std::make_unique<GlobalIndexWAL>()) {}

GlobalIndex::~GlobalIndex() {
    if (is_open_) {
        close();
    }
}

// --- Lifecycle ---

Status GlobalIndex::open(const std::string& db_path, Manifest& manifest,
                          const Options& options) {
    if (is_open_) {
        return Status::InvalidArgument("Already open");
    }
    db_path_ = db_path;
    options_ = options;

    GlobalIndexWAL::Options wal_opts;
    Status s = wal_->open(db_path, manifest, wal_opts);
    if (!s.ok()) return s;

    is_open_ = true;
    return Status::OK();
}

Status GlobalIndex::close() {
    if (!is_open_) {
        return Status::OK();
    }
    // Persist index to snapshot before closing.
    Status s = saveSnapshot(snapshotPath());
    if (!s.ok()) {
        wal_->close();
        is_open_ = false;
        return s;
    }
    wal_->close();
    is_open_ = false;
    return Status::OK();
}

bool GlobalIndex::isOpen() const {
    return is_open_;
}

// --- Index Operations ---

Status GlobalIndex::put(const std::string& key, uint64_t packed_version, uint32_t segment_id) {
    Status s = wal_->appendPut(key, packed_version, segment_id, WalProducer::kWB);
    if (!s.ok()) return s;
    applyPut(key, packed_version, segment_id);
    updates_since_snapshot_++;
    return Status::OK();
}

Status GlobalIndex::putChecked(const std::string& key, uint64_t packed_version,
                               uint32_t segment_id, const KeyResolver& resolver) {
    Status s = wal_->appendPut(key, packed_version, segment_id, WalProducer::kWB);
    if (!s.ok()) return s;
    if (dht_.addEntryChecked(key, packed_version, segment_id, resolver)) {
        ++key_count_;
    }
    updates_since_snapshot_++;
    return Status::OK();
}

bool GlobalIndex::get(const std::string& key,
                  std::vector<uint32_t>& segment_ids,
                  std::vector<uint64_t>& packed_versions) const {
    std::vector<uint64_t> pvs;
    std::vector<uint32_t> ids;
    if (!dht_.findAll(key, pvs, ids)) return false;
    packed_versions = std::move(pvs);
    segment_ids = std::move(ids);
    return true;
}

bool GlobalIndex::get(const std::string& key, uint64_t upper_bound,
                  uint64_t& packed_version, uint32_t& segment_id) const {
    std::vector<uint64_t> pvs;
    std::vector<uint32_t> ids;
    if (!dht_.findAll(key, pvs, ids)) return false;
    // Entries are sorted by packed_version descending; find the first <= upper_bound.
    for (size_t i = 0; i < pvs.size(); ++i) {
        if (pvs[i] <= upper_bound) {
            packed_version = pvs[i];
            segment_id = ids[i];
            return true;
        }
    }
    return false;
}

Status GlobalIndex::getLatest(const std::string& key,
                        uint64_t& packed_version, uint32_t& segment_id) const {
    uint64_t pv;
    uint32_t id;
    if (!dht_.findFirst(key, pv, id)) {
        return Status::NotFound(key);
    }
    packed_version = pv;
    segment_id = id;
    return Status::OK();
}

bool GlobalIndex::contains(const std::string& key) const {
    return dht_.contains(key);
}

Status GlobalIndex::relocate(const std::string& key, uint64_t packed_version,
                              uint32_t old_segment_id, uint32_t new_segment_id) {
    Status s = wal_->appendRelocate(key, packed_version, old_segment_id, new_segment_id, WalProducer::kGC);
    if (!s.ok()) return s;
    applyRelocate(key, packed_version, old_segment_id, new_segment_id);
    updates_since_snapshot_++;
    return Status::OK();
}

Status GlobalIndex::eliminate(const std::string& key, uint64_t packed_version,
                               uint32_t segment_id) {
    Status s = wal_->appendEliminate(key, packed_version, segment_id, WalProducer::kGC);
    if (!s.ok()) return s;
    applyEliminate(key, packed_version, segment_id);
    updates_since_snapshot_++;
    return Status::OK();
}

// --- Iteration ---

void GlobalIndex::forEachGroup(
    const std::function<void(uint64_t hash,
                             const std::vector<uint64_t>& packed_versions,
                             const std::vector<uint32_t>& segment_ids)>& fn) const {
    // DHT stores (packed_versions, ids=segment_ids) — matches caller's signature
    dht_.forEachGroup([&fn](uint64_t hash,
                            const std::vector<uint64_t>& pvs,
                            const std::vector<uint32_t>& ids) {
        fn(hash, pvs, ids);
    });
}

void GlobalIndex::clear() {
    dht_.clear();
    key_count_ = 0;
}

// --- WAL commit ---

Status GlobalIndex::commitWB() {
    return wal_->commit(WalProducer::kWB);
}

Status GlobalIndex::commitGC() {
    return wal_->commit(WalProducer::kGC);
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
static constexpr uint32_t kVersion = 7;

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

    // Per entry: [hash:8][packed_version:8][segment_id:4]
    dht_.forEach([&writer](uint64_t hash, uint64_t packed_version, uint32_t id) {
        writer.writeVal(hash);
        writer.writeVal(packed_version);
        writer.writeVal(id);
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

    // Format version
    uint32_t format_version;
    if (!reader.readVal(format_version) || format_version != kVersion) {
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
        uint64_t packed_ver;
        uint32_t seg;
        if (!reader.readVal(hash) || !reader.readVal(packed_ver) || !reader.readVal(seg)) {
            return Status::Corruption("Failed to read entry");
        }
        dht_.addEntryByHash(hash, packed_ver, seg);
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

void GlobalIndex::applyPut(std::string_view key, uint64_t packed_version,
                           uint32_t segment_id) {
    if (dht_.addEntryIsNew(key, packed_version, segment_id)) {
        ++key_count_;
    }
}

void GlobalIndex::applyRelocate(std::string_view key, uint64_t packed_version,
                                uint32_t old_segment_id, uint32_t new_segment_id) {
    dht_.updateEntryId(key, packed_version, old_segment_id, new_segment_id);
}

void GlobalIndex::applyEliminate(std::string_view key, uint64_t packed_version,
                                 uint32_t segment_id) {
    if (dht_.removeEntry(key, packed_version, segment_id)) {
        --key_count_;
    }
}

Status GlobalIndex::maybeSnapshot() {
    // Stub: no auto-snapshot yet.
    return Status::OK();
}

std::string GlobalIndex::snapshotPath() const {
    return db_path_ + "/gi/snapshot";
}

}  // namespace internal
}  // namespace kvlite
