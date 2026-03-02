#include "internal/global_index.h"

#include <algorithm>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <unistd.h>

#include "internal/crc32.h"
#include "internal/global_index_wal.h"
#include "internal/manifest.h"
#include "internal/wal_stream.h"

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

    // Load persisted max_version from last savepoint (if any).
    std::string mv_str;
    if (manifest_.get(ManifestKey::kGiSavepointMaxVersion, mv_str) && !mv_str.empty()) {
        max_version_.store(std::stoull(mv_str), std::memory_order_relaxed);
    }

    s = recover();
    if (!s.ok()) {
        wal_->close();
        return s;
    }

    is_open_ = true;
    return Status::OK();
}

Status GlobalIndex::recover() {
    namespace fs = std::filesystem;

    // Re-truncate WAL: delete files with max_version <= savepoint max_version.
    // Idempotent — safe if the previous truncation partially completed.
    Status s = wal_->truncate(max_version_.load(std::memory_order_relaxed));
    if (!s.ok()) return s;

    // Recover from partial savepoint swap: if .old exists but valid dir
    // doesn't, the swap was interrupted — restore the old savepoint.
    std::string valid_dir = savepointDir();
    std::string old_dir = valid_dir + ".old";
    std::string tmp_dir = valid_dir + ".tmp";
    std::error_code ec;
    fs::remove_all(tmp_dir, ec);  // incomplete write, discard
    if (!fs::exists(valid_dir, ec) && fs::exists(old_dir, ec)) {
        fs::rename(old_dir, valid_dir, ec);
    }
    fs::remove_all(old_dir, ec);  // clean up leftover

    // Load savepoint if one exists.
    if (fs::exists(valid_dir, ec) && fs::is_directory(valid_dir, ec)) {
        s = readSavepoint(valid_dir);
        if (!s.ok()) return s;
    }

    // Determine the valid segment range from the Manifest.
    // WAL entries referencing segments beyond max_seg_id are orphans
    // from a flush that crashed before the Manifest commit point.
    std::string seg_val;
    bool has_max_seg = manifest_.get(ManifestKey::kSegmentsMaxSegId, seg_val);
    uint32_t max_seg_id = has_max_seg
        ? static_cast<uint32_t>(std::stoul(seg_val)) : 0;

    // Replay all remaining WAL records, ordered by file (= by max_version).
    // Skip entries that reference unregistered segments.
    auto stream = wal_->replayStream();
    while (stream->valid()) {
        const auto& rec = stream->record();
        switch (rec.op) {
            case WalOp::kPut:
                if (!has_max_seg || rec.segment_id > max_seg_id) break;
                applyPut(rec.hkey, rec.packed_version, rec.segment_id);
                {
                    uint64_t cur = max_version_.load(std::memory_order_relaxed);
                    if (rec.packed_version > cur)
                        max_version_.store(rec.packed_version, std::memory_order_relaxed);
                }
                break;
            case WalOp::kRelocate:
                if (!has_max_seg || rec.new_segment_id > max_seg_id) break;
                applyRelocate(rec.hkey, rec.packed_version,
                              rec.segment_id, rec.new_segment_id);
                break;
            case WalOp::kEliminate:
                applyEliminate(rec.hkey, rec.packed_version, rec.segment_id);
                break;
        }
        s = stream->next();
        if (!s.ok()) return s;
    }

    // Converge: write a fresh savepoint capturing the fully recovered state,
    // then truncate the WAL entirely. This eliminates any WAL dependency
    // and ensures a clean starting state.
    s = writeSavepoint(tmp_dir);
    if (!s.ok()) return s;

    fs::remove_all(old_dir, ec);
    if (fs::exists(valid_dir, ec)) {
        fs::rename(valid_dir, old_dir, ec);
    }
    fs::rename(tmp_dir, valid_dir, ec);
    fs::remove_all(old_dir, ec);

    s = manifest_.set(ManifestKey::kGiSavepointMaxVersion,
                      std::to_string(max_version_.load(std::memory_order_relaxed)));
    if (!s.ok()) return s;

    s = wal_->truncate(max_version_.load(std::memory_order_relaxed));
    if (!s.ok()) return s;

    // Start a fresh WAL file.
    return wal_->startNewFile();
}

Status GlobalIndex::close() {
    if (!is_open_) {
        return Status::OK();
    }
    Status s = storeSavepoint(max_version_.load(std::memory_order_relaxed));
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
    std::shared_lock<std::shared_mutex> lock(savepoint_mu_);
    Status s = wal_->appendPut(hkey, packed_version, segment_id, WalProducer::kWB);
    if (!s.ok()) return s;
    applyPut(hkey, packed_version, segment_id);
    uint64_t cur = max_version_.load(std::memory_order_relaxed);
    while (packed_version > cur &&
           !max_version_.compare_exchange_weak(cur, packed_version,
               std::memory_order_relaxed)) {}
    wal_->updateMaxVersion(packed_version);
    return Status::OK();
}

Status GlobalIndex::stagePut(uint64_t hkey, uint64_t packed_version, uint32_t segment_id) {
    applyPut(hkey, packed_version, segment_id);  // DHT first
    Status s = wal_->stagePut(hkey, packed_version, segment_id, WalProducer::kWB);
    if (!s.ok()) return s;
    uint64_t cur = max_version_.load(std::memory_order_relaxed);
    while (packed_version > cur &&
           !max_version_.compare_exchange_weak(cur, packed_version,
               std::memory_order_relaxed)) {}
    wal_->updateMaxVersion(packed_version);
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
    // Caller must hold a BatchGuard.
    Status s = wal_->stageRelocate(hkey, packed_version, old_segment_id, new_segment_id, WalProducer::kGC);
    if (!s.ok()) return s;
    applyRelocate(hkey, packed_version, old_segment_id, new_segment_id);
    return Status::OK();
}

Status GlobalIndex::eliminate(uint64_t hkey, uint64_t packed_version,
                               uint32_t segment_id) {
    // Caller must hold a BatchGuard.
    Status s = wal_->stageEliminate(hkey, packed_version, segment_id, WalProducer::kGC);
    if (!s.ok()) return s;
    applyEliminate(hkey, packed_version, segment_id);
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
    key_count_.store(0, std::memory_order_relaxed);
}

// --- WAL commit ---

Status GlobalIndex::commitWB(uint64_t max_version) {
    uint64_t cur = max_version_.load(std::memory_order_relaxed);
    while (max_version > cur &&
           !max_version_.compare_exchange_weak(cur, max_version,
               std::memory_order_relaxed)) {}
    wal_->updateMaxVersion(max_version);
    return wal_->commit(WalProducer::kWB);
}

Status GlobalIndex::commitGC() {
    // Caller must hold a BatchGuard.
    return wal_->commit(WalProducer::kGC);
}

// --- Binary savepoint ---

Status GlobalIndex::storeSavepoint(uint64_t /*savepoint_version*/) {
    namespace fs = std::filesystem;
    std::unique_lock<std::shared_mutex> lock(savepoint_mu_);

    uint64_t snapshot_version = max_version_.load(std::memory_order_relaxed);

    std::string valid_dir = savepointDir();
    std::string tmp_dir = valid_dir + ".tmp";
    std::string old_dir = valid_dir + ".old";

    // Write savepoint to temporary directory.
    Status s = writeSavepoint(tmp_dir);
    if (!s.ok()) return s;

    // Persist snapshot_version to Manifest BEFORE the atomic swap.
    // If we crash after this but before the swap completes, recovery will
    // over-truncate WAL (safe: those records are captured in the new savepoint
    // that will be discarded), load the old savepoint, and replay — correct
    // because WAL replay is idempotent and recovery writes a fresh savepoint.
    s = manifest_.set(ManifestKey::kGiSavepointMaxVersion,
                      std::to_string(snapshot_version));
    if (!s.ok()) return s;

    // Atomic swap: valid → old, tmp → valid, remove old.
    // Recovery handles partial completion (see recover()).
    std::error_code ec;
    if (fs::exists(valid_dir, ec)) {
        fs::remove_all(old_dir, ec);  // clean up any leftover
        fs::rename(valid_dir, old_dir, ec);
        if (ec) {
            return Status::IOError("Failed to move old savepoint: " + ec.message());
        }
    }
    fs::rename(tmp_dir, valid_dir, ec);
    if (ec) {
        // Try to restore old savepoint.
        std::error_code ec2;
        if (fs::exists(old_dir, ec2)) {
            fs::rename(old_dir, valid_dir, ec2);
        }
        return Status::IOError("Failed to install savepoint: " + ec.message());
    }
    fs::remove_all(old_dir, ec);  // best-effort cleanup

    s = wal_->truncateForSavepoint(snapshot_version);
    if (!s.ok()) return s;

    // Start a fresh WAL file so replayed records from the old active
    // file don't duplicate the savepoint data on next recovery.
    return wal_->startNewFile();
}

// --- Statistics ---

size_t GlobalIndex::keyCount() const {
    return key_count_.load(std::memory_order_relaxed);
}

size_t GlobalIndex::entryCount() const {
    return dht_.size();
}

size_t GlobalIndex::memoryUsage() const {
    return dht_.memoryUsage();
}

double GlobalIndex::estimateDeadRatio() const {
    size_t total_groups = 0;
    size_t multi_version_groups = 0;
    forEachGroup([&](uint64_t, const auto& pvs, const auto&) {
        ++total_groups;
        if (pvs.size() > 1) ++multi_version_groups;
    });
    if (total_groups == 0) return 0.0;
    return static_cast<double>(multi_version_groups) /
           static_cast<double>(total_groups);
}

// --- Persistence ---

static constexpr char kMagic[4] = {'L', '1', 'I', 'X'};
static constexpr uint32_t kFormatVersion = 11;
static constexpr uint64_t kMaxFileSize = 1ULL << 30;  // 1 GB


// --- Savepoint persistence (binary, multi-file) ---
//
// Multi-file binary savepoint format (v11, per-bucket chains):
//
// Directory: <db_path>/gi/savepoint/
// Files: 00000000.dat, 00000001.dat, ...
//
// Each file:
//   Global header (33 bytes) + File header (12 bytes) +
//   Per-bucket chain records +
//   CRC32 footer (4 bytes)
//
// Per-bucket record:
//   chain_len (uint8_t): 0 = empty, 1+ = main + extensions
//   if chain_len > 0: chain_len × stride bytes of bucket data

std::vector<GlobalIndex::SavepointFileDesc> GlobalIndex::computeFileLayout() const {
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

    std::vector<SavepointFileDesc> files;
    uint32_t bs = 0, fi = 0;
    while (bs < num_buckets) {
        uint32_t cnt = std::min(buckets_per_file, num_buckets - bs);
        files.push_back({fi, bs, cnt, bs + cnt >= num_buckets});
        bs += cnt;
        fi++;
    }
    return files;
}

Status GlobalIndex::writeSavepointFile(const std::string& dir,
                                      const SavepointFileDesc& fd) const {
    char fname[16];
    std::snprintf(fname, sizeof(fname), "%08u.dat", fd.file_index);
    std::string fpath = dir + "/" + fname;

    std::ofstream file(fpath, std::ios::binary);
    if (!file) {
        return Status::IOError("Failed to create savepoint file: " + fpath);
    }

    const auto& cfg = dht_.config();
    uint64_t num_entries = dht_.size();
    uint64_t key_count = key_count_.load(std::memory_order_relaxed);
    uint32_t ext_count = 0;  // extensions are inline in chain data

    CRC32Writer writer(file);

    // Global header (33 bytes)
    writer.write(kMagic, 4);
    writer.writeVal(kFormatVersion);
    writer.writeVal(num_entries);
    writer.writeVal(key_count);
    writer.writeVal(cfg.bucket_bits);
    writer.writeVal(cfg.bucket_bytes);
    writer.writeVal(ext_count);

    // File-specific header (12 bytes)
    writer.writeVal(fd.file_index);
    writer.writeVal(fd.bucket_start);
    writer.writeVal(fd.bucket_count);

    // Per-bucket chain records.
    std::vector<uint8_t> chain_buf;
    uint32_t bucket_end = fd.bucket_start + fd.bucket_count;
    for (uint32_t bi = fd.bucket_start; bi < bucket_end; ++bi) {
        uint32_t chain_len = dht_.snapshotBucketChain(bi, chain_buf);
        uint8_t cl8 = static_cast<uint8_t>(chain_len);
        writer.writeVal(cl8);
        if (chain_len > 0) {
            writer.write(chain_buf.data(), chain_buf.size());
        }
    }

    // CRC32 footer
    uint32_t checksum = writer.finalize();
    file.write(reinterpret_cast<const char*>(&checksum), sizeof(checksum));

    if (!file.good()) {
        return Status::IOError("Failed to write savepoint file: " + fpath);
    }
    file.flush();

    // fsync the file so data is durable before the directory rename.
    int sync_fd = ::open(fpath.c_str(), O_RDONLY);
    if (sync_fd >= 0) {
        ::fdatasync(sync_fd);
        ::close(sync_fd);
    }
    return Status::OK();
}

Status GlobalIndex::writeSavepoint(const std::string& dir) const {
    namespace fs = std::filesystem;

    std::error_code ec;
    if (fs::exists(dir, ec)) {
        fs::remove_all(dir, ec);
    }
    fs::create_directories(dir, ec);
    if (ec) {
        return Status::IOError("Failed to create savepoint dir: " + dir + ": " + ec.message());
    }

    auto files = computeFileLayout();
    for (const auto& fd : files) {
        Status s = writeSavepointFile(dir, fd);
        if (!s.ok()) return s;
    }

    // fsync the directory so all file entries are durable before rename.
    int dfd = ::open(dir.c_str(), O_RDONLY);
    if (dfd >= 0) {
        ::fsync(dfd);
        ::close(dfd);
    }
    return Status::OK();
}

Status GlobalIndex::readSavepointFile(const std::string& fpath,
                                     uint32_t stride,
                                     uint64_t& out_entries,
                                     uint64_t& out_key_count,
                                     uint32_t& out_ext_count) {
    std::ifstream file(fpath, std::ios::binary);
    if (!file) {
        return Status::IOError("Failed to open savepoint file: " + fpath);
    }

    CRC32Reader reader(file);

    // Global header (33 bytes)
    char magic[4];
    if (!reader.read(magic, 4) || std::memcmp(magic, kMagic, 4) != 0) {
        return Status::Corruption("Invalid savepoint magic in: " + fpath);
    }
    uint32_t format_version;
    if (!reader.readVal(format_version) || format_version != kFormatVersion) {
        return Status::Corruption("Unsupported savepoint version in: " + fpath);
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

    // Read per-bucket chain records.
    std::vector<uint8_t> chain_buf;
    for (uint32_t i = 0; i < bucket_count; ++i) {
        uint8_t chain_len;
        if (!reader.readVal(chain_len)) {
            return Status::Corruption("Failed to read chain_len in: " + fpath);
        }
        if (chain_len > 0) {
            size_t data_size = static_cast<size_t>(chain_len) * stride;
            chain_buf.resize(data_size);
            if (!reader.read(chain_buf.data(), data_size)) {
                return Status::Corruption("Failed to read chain data in: " + fpath);
            }
            dht_.loadBucketChain(bucket_start + i, chain_buf.data(), chain_len);
        }
        // chain_len == 0: bucket is empty, already zeroed from clear()
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

Status GlobalIndex::readSavepoint(const std::string& dir) {
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
        return Status::Corruption("No savepoint files in directory: " + dir);
    }

    clear();

    uint32_t stride = dht_.bucketStride();
    uint64_t entries = 0, key_count = 0;
    uint32_t ext_count = 0;

    for (const auto& fpath : files) {
        Status s = readSavepointFile(fpath, stride, entries, key_count, ext_count);
        if (!s.ok()) return s;
    }

    dht_.setSize(entries);
    key_count_ = static_cast<size_t>(key_count);
    return Status::OK();
}

// --- Private ---

void GlobalIndex::applyPut(uint64_t hkey, uint64_t packed_version,
                           uint32_t segment_id) {
    if (dht_.addEntryIsNew(hkey, packed_version, segment_id)) {
        key_count_.fetch_add(1, std::memory_order_relaxed);
    }
}

void GlobalIndex::applyRelocate(uint64_t hkey, uint64_t packed_version,
                                uint32_t old_segment_id, uint32_t new_segment_id) {
    dht_.updateEntryId(hkey, packed_version, old_segment_id, new_segment_id);
}

void GlobalIndex::applyEliminate(uint64_t hkey, uint64_t packed_version,
                                 uint32_t segment_id) {
    if (dht_.removeEntry(hkey, packed_version, segment_id)) {
        key_count_.fetch_sub(1, std::memory_order_relaxed);
    }
}

Status GlobalIndex::maybeSavepoint() {
    if (wal_->size() == 0) {
        return Status::OK();
    }
    return storeSavepoint(max_version_.load(std::memory_order_relaxed));
}

std::string GlobalIndex::savepointDir() const {
    return db_path_ + "/gi/savepoint";
}

}  // namespace internal
}  // namespace kvlite
