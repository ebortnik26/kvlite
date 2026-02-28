#include "internal/global_index.h"

#include <algorithm>
#include <atomic>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <thread>

#include "internal/crc32.h"
#include "internal/global_index_wal.h"
#include "internal/manifest.h"

namespace kvlite {
namespace internal {

// CRC-accumulating I/O wrappers using the shared crc32.h implementation.
struct CRC32Writer {
    std::ofstream& out;
    uint32_t crc = 0xFFFFFFFFu;

    explicit CRC32Writer(std::ofstream& o) : out(o) {}

    bool write(const void* data, size_t len) {
        crc = updateCrc32(crc, data, len);
        out.write(reinterpret_cast<const char*>(data), len);
        return out.good();
    }

    template<typename T>
    bool writeVal(const T& val) { return write(&val, sizeof(val)); }

    uint32_t finalize() const { return finalizeCrc32(crc); }
};

struct CRC32Reader {
    std::ifstream& in;
    uint32_t crc = 0xFFFFFFFFu;

    explicit CRC32Reader(std::ifstream& i) : in(i) {}

    bool read(void* data, size_t len) {
        in.read(reinterpret_cast<char*>(data), len);
        if (!in.good()) return false;
        crc = updateCrc32(crc, data, len);
        return true;
    }

    template<typename T>
    bool readVal(T& val) { return read(&val, sizeof(val)); }

    uint32_t finalize() const { return finalizeCrc32(crc); }
};

GlobalIndex::GlobalIndex(Manifest& manifest)
    : manifest_(manifest),
      wal_(std::make_unique<GlobalIndexWAL>()) {}

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

    GlobalIndexWAL::Options wal_opts;
    Status s = wal_->open(db_path, manifest_, wal_opts);
    if (!s.ok()) return s;

    is_open_ = true;
    return Status::OK();
}

Status GlobalIndex::close() {
    if (!is_open_) {
        return Status::OK();
    }
    // Persist index to v7 snapshot as fallback.
    // For fast binary snapshots, the caller should invoke snapshot(version)
    // before close().
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

Status GlobalIndex::put(uint64_t hkey, uint64_t packed_version, uint32_t segment_id) {
    std::shared_lock<std::shared_mutex> lock(snapshot_mu_);
    Status s = wal_->appendPut(hkey, packed_version, segment_id, WalProducer::kWB);
    if (!s.ok()) return s;
    applyPut(hkey, packed_version, segment_id);
    updates_since_snapshot_++;
    return Status::OK();
}

bool GlobalIndex::get(uint64_t hkey,
                  std::vector<uint32_t>& segment_ids,
                  std::vector<uint64_t>& packed_versions) const {
    std::vector<uint64_t> pvs;
    std::vector<uint32_t> ids;
    if (!dht_.findAll(hkey, pvs, ids)) return false;
    packed_versions = std::move(pvs);
    segment_ids = std::move(ids);
    return true;
}

bool GlobalIndex::get(uint64_t hkey, uint64_t upper_bound,
                  uint64_t& packed_version, uint32_t& segment_id) const {
    std::vector<uint64_t> pvs;
    std::vector<uint32_t> ids;
    if (!dht_.findAll(hkey, pvs, ids)) return false;
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

Status GlobalIndex::getLatest(uint64_t hkey,
                        uint64_t& packed_version, uint32_t& segment_id) const {
    uint64_t pv;
    uint32_t id;
    if (!dht_.findFirst(hkey, pv, id)) {
        return Status::NotFound("key");
    }
    packed_version = pv;
    segment_id = id;
    return Status::OK();
}

bool GlobalIndex::contains(uint64_t hkey) const {
    return dht_.contains(hkey);
}

Status GlobalIndex::relocate(uint64_t hkey, uint64_t packed_version,
                              uint32_t old_segment_id, uint32_t new_segment_id) {
    std::shared_lock<std::shared_mutex> lock(snapshot_mu_);
    Status s = wal_->appendRelocate(hkey, packed_version, old_segment_id, new_segment_id, WalProducer::kGC);
    if (!s.ok()) return s;
    applyRelocate(hkey, packed_version, old_segment_id, new_segment_id);
    updates_since_snapshot_++;
    return Status::OK();
}

Status GlobalIndex::eliminate(uint64_t hkey, uint64_t packed_version,
                               uint32_t segment_id) {
    std::shared_lock<std::shared_mutex> lock(snapshot_mu_);
    Status s = wal_->appendEliminate(hkey, packed_version, segment_id, WalProducer::kGC);
    if (!s.ok()) return s;
    applyEliminate(hkey, packed_version, segment_id);
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
    std::shared_lock<std::shared_mutex> lock(snapshot_mu_);
    return wal_->commit(WalProducer::kWB);
}

Status GlobalIndex::commitGC() {
    std::shared_lock<std::shared_mutex> lock(snapshot_mu_);
    return wal_->commit(WalProducer::kGC);
}

// --- Binary snapshot ---

Status GlobalIndex::storeSnapshot(uint64_t snapshot_version) {
    namespace fs = std::filesystem;
    std::unique_lock<std::shared_mutex> lock(snapshot_mu_);

    // Record the DB version at the moment the exclusive lock is acquired.
    // Used during recovery to determine which WAL entries to replay.
    Status ts = manifest_.set("gi.snapshot.version", std::to_string(snapshot_version));
    if (!ts.ok()) return ts;

    std::string valid_dir = snapshotDirV9();
    std::string tmp_dir = valid_dir + ".tmp";
    std::string old_dir = valid_dir + ".old";

    // Write snapshot to temporary directory.
    Status s = saveBinarySnapshot(tmp_dir);
    if (!s.ok()) return s;

    // Atomic swap: valid → old, tmp → valid, remove old.
    std::error_code ec;
    if (fs::exists(valid_dir, ec)) {
        fs::remove_all(old_dir, ec);  // clean up any leftover
        fs::rename(valid_dir, old_dir, ec);
        if (ec) {
            return Status::IOError("Failed to move old snapshot: " + ec.message());
        }
    }
    fs::rename(tmp_dir, valid_dir, ec);
    if (ec) {
        // Try to restore old snapshot.
        std::error_code ec2;
        if (fs::exists(old_dir, ec2)) {
            fs::rename(old_dir, valid_dir, ec2);
        }
        return Status::IOError("Failed to install snapshot: " + ec.message());
    }
    fs::remove_all(old_dir, ec);  // best-effort cleanup

    s = wal_->truncate();
    if (!s.ok()) return s;
    updates_since_snapshot_ = 0;
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

// --- Persistence (v7) ---

static constexpr char kMagic[4] = {'L', '1', 'I', 'X'};
static constexpr uint32_t kVersionV7 = 7;
static constexpr uint32_t kVersionV9 = 9;
static constexpr uint64_t kMaxFileSize = 1ULL << 30;  // 1 GB

Status GlobalIndex::saveSnapshot(const std::string& path) const {
    std::ofstream file(path, std::ios::binary);
    if (!file) {
        return Status::IOError("Failed to create snapshot file: " + path);
    }

    CRC32Writer writer(file);

    // Header
    writer.write(kMagic, 4);
    writer.writeVal(kVersionV7);

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

// --- Persistence (v8 binary, multi-file) ---
//
// Multi-file binary snapshot format (v8):
//
// Directory: <db_path>/gi/snapshot.v9/
// Files: 00000000.dat, 00000001.dat, ...
//
// Each file:
//   Global header (29 bytes) + File header (12 bytes) +
//   Main arena data (bucket_count × stride bytes) +
//   Extension data (last file only, ext_count × stride bytes) +
//   CRC32 footer (4 bytes)

std::vector<GlobalIndex::SnapshotFileDesc> GlobalIndex::computeFileLayout() const {
    static constexpr size_t kOverhead = 33 + 12 + 4;  // headers + footer

    uint32_t num_buckets = dht_.numBuckets();
    uint32_t stride = dht_.bucketStride();

    uint32_t buckets_per_file;
    if (stride == 0) {
        buckets_per_file = num_buckets;
    } else {
        uint64_t budget = kMaxFileSize - kOverhead;
        buckets_per_file = static_cast<uint32_t>(
            std::min(static_cast<uint64_t>(num_buckets), budget / stride));
        if (buckets_per_file == 0) buckets_per_file = 1;
    }

    std::vector<SnapshotFileDesc> files;
    uint32_t bs = 0, fi = 0;
    while (bs < num_buckets) {
        uint32_t cnt = std::min(buckets_per_file, num_buckets - bs);
        files.push_back({fi, bs, cnt, bs + cnt >= num_buckets});
        bs += cnt;
        fi++;
    }
    return files;
}

Status GlobalIndex::writeSnapshotFile(const std::string& dir,
                                      const SnapshotFileDesc& fd) const {
    char fname[16];
    std::snprintf(fname, sizeof(fname), "%08u.dat", fd.file_index);
    std::string fpath = dir + "/" + fname;

    std::ofstream file(fpath, std::ios::binary);
    if (!file) {
        return Status::IOError("Failed to create snapshot file: " + fpath);
    }

    const auto& cfg = dht_.config();
    uint32_t stride = dht_.bucketStride();
    uint32_t ext_count = dht_.extCount();
    uint64_t num_entries = dht_.size();
    uint64_t key_count = key_count_;

    CRC32Writer writer(file);

    // Global header (33 bytes)
    writer.write(kMagic, 4);
    writer.writeVal(kVersionV9);
    writer.writeVal(num_entries);
    writer.writeVal(key_count);
    writer.writeVal(cfg.bucket_bits);
    writer.writeVal(cfg.bucket_bytes);
    writer.writeVal(ext_count);

    // File-specific header (12 bytes)
    writer.writeVal(fd.file_index);
    writer.writeVal(fd.bucket_start);
    writer.writeVal(fd.bucket_count);

    // Main arena data
    const uint8_t* arena = dht_.arenaData();
    size_t arena_offset = static_cast<size_t>(fd.bucket_start) * stride;
    size_t arena_len = static_cast<size_t>(fd.bucket_count) * stride;
    writer.write(arena + arena_offset, arena_len);

    // Extension data (only in last file)
    if (fd.is_last && ext_count > 0) {
        for (uint32_t i = 1; i <= ext_count; ++i) {
            writer.write(dht_.extSlotData(i), stride);
        }
    }

    // CRC32 footer
    uint32_t checksum = writer.finalize();
    file.write(reinterpret_cast<const char*>(&checksum), sizeof(checksum));

    if (!file.good()) {
        return Status::IOError("Failed to write snapshot file: " + fpath);
    }
    return Status::OK();
}

Status GlobalIndex::saveBinarySnapshot(const std::string& dir) const {
    namespace fs = std::filesystem;

    std::error_code ec;
    if (fs::exists(dir, ec)) {
        fs::remove_all(dir, ec);
    }
    fs::create_directories(dir, ec);
    if (ec) {
        return Status::IOError("Failed to create snapshot dir: " + dir + ": " + ec.message());
    }

    auto files = computeFileLayout();

    uint32_t num_threads = std::min(options_.snapshot_threads,
                                     static_cast<uint32_t>(files.size()));
    if (num_threads <= 1) {
        for (const auto& fd : files) {
            Status s = writeSnapshotFile(dir, fd);
            if (!s.ok()) return s;
        }
        return Status::OK();
    }

    // Parallel path: workers grab files from an atomic counter.
    std::atomic<uint32_t> next_file{0};
    std::atomic<bool> has_error{false};
    Status first_error;
    std::mutex error_mu;

    auto worker = [&]() {
        while (!has_error.load(std::memory_order_relaxed)) {
            uint32_t fi = next_file.fetch_add(1, std::memory_order_relaxed);
            if (fi >= files.size()) return;
            Status s = writeSnapshotFile(dir, files[fi]);
            if (!s.ok()) {
                std::lock_guard<std::mutex> lock(error_mu);
                if (!has_error.exchange(true, std::memory_order_relaxed)) {
                    first_error = s;
                }
                return;
            }
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (uint32_t i = 0; i < num_threads; ++i) {
        threads.emplace_back(worker);
    }
    for (auto& t : threads) {
        t.join();
    }

    return has_error.load() ? first_error : Status::OK();
}

Status GlobalIndex::loadSnapshotFile(const std::string& fpath,
                                     uint32_t stride,
                                     uint64_t& out_entries,
                                     uint64_t& out_key_count,
                                     uint32_t& out_ext_count) {
    std::ifstream file(fpath, std::ios::binary);
    if (!file) {
        return Status::IOError("Failed to open snapshot file: " + fpath);
    }

    CRC32Reader reader(file);

    // Global header (33 bytes)
    char magic[4];
    if (!reader.read(magic, 4) || std::memcmp(magic, kMagic, 4) != 0) {
        return Status::Corruption("Invalid snapshot magic in: " + fpath);
    }
    uint32_t format_version;
    if (!reader.readVal(format_version) || format_version != kVersionV9) {
        return Status::Corruption("Unsupported snapshot version in: " + fpath);
    }

    uint8_t bucket_bits;
    uint32_t bucket_bytes, ext_count;
    if (!reader.readVal(out_entries) || !reader.readVal(out_key_count) ||
        !reader.readVal(bucket_bits) ||
        !reader.readVal(bucket_bytes) || !reader.readVal(ext_count)) {
        return Status::Corruption("Failed to read header in: " + fpath);
    }
    out_ext_count = ext_count;

    const auto& cfg = dht_.config();
    if (bucket_bits != cfg.bucket_bits ||
        bucket_bytes != cfg.bucket_bytes) {
        return Status::Corruption("Snapshot config mismatch in: " + fpath);
    }

    // File-specific header (12 bytes)
    uint32_t file_index, bucket_start, bucket_count;
    if (!reader.readVal(file_index) || !reader.readVal(bucket_start) ||
        !reader.readVal(bucket_count)) {
        return Status::Corruption("Failed to read file header in: " + fpath);
    }

    // Read main arena data directly into the arena.
    size_t arena_offset = static_cast<size_t>(bucket_start) * stride;
    size_t arena_len = static_cast<size_t>(bucket_count) * stride;
    uint8_t* arena_dst = const_cast<uint8_t*>(dht_.arenaData()) + arena_offset;
    if (!reader.read(arena_dst, arena_len)) {
        return Status::Corruption("Failed to read arena data in: " + fpath);
    }

    // Extension data (only in last file)
    bool is_last = (bucket_start + bucket_count >= dht_.numBuckets());
    if (is_last && ext_count > 0) {
        std::vector<uint8_t> ext_buf(static_cast<size_t>(ext_count) * stride);
        if (!reader.read(ext_buf.data(), ext_buf.size())) {
            return Status::Corruption("Failed to read extension data in: " + fpath);
        }
        dht_.loadExtensions(ext_buf.data(), ext_count, stride);
    }

    // Verify CRC32 footer
    uint32_t expected_crc = reader.finalize();
    uint32_t stored_crc;
    file.read(reinterpret_cast<char*>(&stored_crc), sizeof(stored_crc));
    if (!file.good()) {
        return Status::Corruption("Failed to read checksum in: " + fpath);
    }
    if (stored_crc != expected_crc) {
        return Status::ChecksumMismatch("Snapshot checksum mismatch in: " + fpath);
    }
    return Status::OK();
}

Status GlobalIndex::loadBinarySnapshot(const std::string& dir) {
    namespace fs = std::filesystem;

    if (!fs::exists(dir) || !fs::is_directory(dir)) {
        return Status::IOError("Snapshot directory not found: " + dir);
    }

    std::vector<std::string> files;
    for (const auto& entry : fs::directory_iterator(dir)) {
        if (entry.path().extension() == ".dat") {
            files.push_back(entry.path().string());
        }
    }
    std::sort(files.begin(), files.end());

    if (files.empty()) {
        return Status::Corruption("No snapshot files in directory: " + dir);
    }

    clear();

    uint32_t stride = dht_.bucketStride();
    uint64_t entries = 0, key_count = 0;
    uint32_t ext_count = 0;

    for (const auto& fpath : files) {
        Status s = loadSnapshotFile(fpath, stride, entries, key_count, ext_count);
        if (!s.ok()) return s;
    }

    dht_.setSize(entries);
    key_count_ = static_cast<size_t>(key_count);
    return Status::OK();
}

Status GlobalIndex::loadV7Snapshot(const std::string& path) {
    std::ifstream file(path, std::ios::binary);
    if (!file) {
        return Status::IOError("Failed to open snapshot file: " + path);
    }

    CRC32Reader reader(file);

    char magic[4];
    if (!reader.read(magic, 4) || std::memcmp(magic, kMagic, 4) != 0) {
        return Status::Corruption("Invalid snapshot magic");
    }
    uint32_t format_version;
    if (!reader.readVal(format_version) || format_version != kVersionV7) {
        return Status::Corruption("Unsupported snapshot version");
    }

    uint64_t num_entries, key_count;
    if (!reader.readVal(num_entries) || !reader.readVal(key_count)) {
        return Status::Corruption("Failed to read v7 header");
    }

    clear();

    for (uint64_t i = 0; i < num_entries; ++i) {
        uint64_t hash, packed_ver;
        uint32_t seg;
        if (!reader.readVal(hash) || !reader.readVal(packed_ver) || !reader.readVal(seg)) {
            return Status::Corruption("Failed to read entry");
        }
        dht_.addEntry(hash, packed_ver, seg);
    }

    key_count_ = static_cast<size_t>(key_count);

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

Status GlobalIndex::loadSnapshot(const std::string& path) {
    namespace fs = std::filesystem;
    std::error_code ec;
    std::string dir_v9 = snapshotDirV9();
    if (fs::exists(dir_v9, ec) && fs::is_directory(dir_v9, ec)) {
        return loadBinarySnapshot(dir_v9);
    }
    return loadV7Snapshot(path);
}

// --- Private ---

void GlobalIndex::applyPut(uint64_t hkey, uint64_t packed_version,
                           uint32_t segment_id) {
    if (dht_.addEntryIsNew(hkey, packed_version, segment_id)) {
        ++key_count_;
    }
}

void GlobalIndex::applyRelocate(uint64_t hkey, uint64_t packed_version,
                                uint32_t old_segment_id, uint32_t new_segment_id) {
    dht_.updateEntryId(hkey, packed_version, old_segment_id, new_segment_id);
}

void GlobalIndex::applyEliminate(uint64_t hkey, uint64_t packed_version,
                                 uint32_t segment_id) {
    if (dht_.removeEntry(hkey, packed_version, segment_id)) {
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

std::string GlobalIndex::snapshotDirV9() const {
    return db_path_ + "/gi/snapshot.v9";
}

}  // namespace internal
}  // namespace kvlite
