#include "internal/global_index_savepoint.h"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <thread>
#include <unistd.h>

#include "internal/crc32.h"
#include "internal/read_write_delta_hash_table.h"

namespace kvlite {
namespace internal {
namespace savepoint {

// CRC-accumulating I/O wrappers.
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

static constexpr char kMagic[4] = {'L', '1', 'I', 'X'};
static constexpr uint32_t kFormatVersion = 11;
static constexpr uint64_t kMaxFileSize = 1ULL << 30;  // 1 GB

std::vector<FileDesc> computeFileLayout(
    const ReadWriteDeltaHashTable& dht, uint8_t num_threads) {
    static constexpr size_t kOverhead = 33 + 12 + 4;  // headers + footer

    uint32_t num_buckets = dht.numBuckets();
    uint32_t stride = dht.bucketStride();
    uint32_t min_files = std::max(static_cast<uint8_t>(1), num_threads);

    uint32_t num_files;
    if (stride == 0) {
        num_files = 1;
    } else {
        uint64_t budget = kMaxFileSize - kOverhead;
        uint32_t max_bpf = static_cast<uint32_t>(
            std::min(static_cast<uint64_t>(num_buckets), budget / stride));
        if (max_bpf == 0) max_bpf = 1;
        uint32_t files_for_size = (num_buckets + max_bpf - 1) / max_bpf;
        num_files = std::max(static_cast<uint32_t>(min_files), files_for_size);
    }
    if (num_files > num_buckets) num_files = num_buckets;

    uint32_t buckets_per_file = (num_buckets + num_files - 1) / num_files;

    std::vector<FileDesc> files;
    uint32_t bs = 0, fi = 0;
    while (bs < num_buckets) {
        uint32_t cnt = std::min(buckets_per_file, num_buckets - bs);
        files.push_back({fi, bs, cnt, bs + cnt >= num_buckets});
        bs += cnt;
        fi++;
    }
    return files;
}

Status storeFile(const std::string& dir, const FileDesc& fd,
                 const ReadWriteDeltaHashTable& dht, uint64_t key_count) {
    char fname[16];
    std::snprintf(fname, sizeof(fname), "%08u.dat", fd.file_index);
    std::string fpath = dir + "/" + fname;

    std::ofstream file(fpath, std::ios::binary);
    if (!file) {
        return Status::IOError("Failed to create savepoint file: " + fpath);
    }

    const auto& cfg = dht.config();
    uint64_t num_entries = dht.size();
    uint32_t ext_count = 0;

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
        uint32_t chain_len = dht.snapshotBucketChain(bi, chain_buf);
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
    return Status::OK();
}

Status store(const std::string& dir, const ReadWriteDeltaHashTable& dht,
             uint64_t key_count, uint8_t num_threads) {
    namespace fs = std::filesystem;

    std::error_code ec;
    if (fs::exists(dir, ec)) {
        fs::remove_all(dir, ec);
    }
    fs::create_directories(dir, ec);
    if (ec) {
        return Status::IOError("Failed to create savepoint dir: " + dir + ": " + ec.message());
    }

    auto files = computeFileLayout(dht, num_threads);
    uint32_t nt = std::min(
        static_cast<uint32_t>(std::max(static_cast<uint8_t>(1), num_threads)),
        static_cast<uint32_t>(files.size()));

    if (nt <= 1) {
        for (const auto& fd : files) {
            Status s = storeFile(dir, fd, dht, key_count);
            if (!s.ok()) return s;
        }
    } else {
        std::vector<Status> statuses(nt, Status::OK());
        std::vector<std::thread> threads;
        threads.reserve(nt - 1);

        auto worker = [&](uint32_t tid) {
            for (uint32_t i = tid; i < files.size(); i += nt) {
                Status s = storeFile(dir, files[i], dht, key_count);
                if (!s.ok()) {
                    statuses[tid] = s;
                    return;
                }
            }
        };

        for (uint32_t t = 1; t < nt; ++t) {
            threads.emplace_back(worker, t);
        }
        worker(0);

        for (auto& th : threads) th.join();

        for (const auto& s : statuses) {
            if (!s.ok()) return s;
        }
    }

    // fsync the directory so all file entries are durable before rename.
    int dfd = ::open(dir.c_str(), O_RDONLY);
    if (dfd >= 0) {
        ::fsync(dfd);
        ::close(dfd);
    }
    return Status::OK();
}

Status loadFile(const std::string& fpath, uint32_t stride,
                ReadWriteDeltaHashTable& dht,
                uint64_t& out_entries, uint64_t& out_key_count,
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

    const auto& cfg = dht.config();
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
            dht.loadBucketChain(bucket_start + i, chain_buf.data(), chain_len);
        }
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

Status load(const std::string& dir, ReadWriteDeltaHashTable& dht,
            std::atomic<size_t>& key_count, uint8_t num_threads) {
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

    dht.clear();
    key_count.store(0, std::memory_order_relaxed);

    uint32_t stride = dht.bucketStride();
    uint32_t nt = std::min(
        static_cast<uint32_t>(std::max(static_cast<uint8_t>(1), num_threads)),
        static_cast<uint32_t>(files.size()));

    struct FileResult {
        uint64_t entries = 0;
        uint64_t key_count = 0;
        uint32_t ext_count = 0;
        Status status;
    };

    if (nt <= 1) {
        uint64_t entries = 0, kc = 0;
        uint32_t ext_count = 0;
        for (const auto& fpath : files) {
            Status s = loadFile(fpath, stride, dht, entries, kc, ext_count);
            if (!s.ok()) return s;
        }
        dht.setSize(entries);
        key_count.store(static_cast<size_t>(kc), std::memory_order_relaxed);
    } else {
        std::vector<FileResult> results(nt);
        std::vector<std::thread> threads;
        threads.reserve(nt - 1);

        auto worker = [&](uint32_t tid) {
            for (uint32_t i = tid; i < files.size(); i += nt) {
                Status s = loadFile(files[i], stride, dht,
                    results[tid].entries, results[tid].key_count,
                    results[tid].ext_count);
                if (!s.ok()) {
                    results[tid].status = s;
                    return;
                }
            }
        };

        for (uint32_t t = 1; t < nt; ++t) {
            threads.emplace_back(worker, t);
        }
        worker(0);

        for (auto& th : threads) th.join();

        for (const auto& r : results) {
            if (!r.status.ok()) return r.status;
        }

        dht.setSize(results[0].entries);
        key_count.store(static_cast<size_t>(results[0].key_count), std::memory_order_relaxed);
    }

    return Status::OK();
}

}  // namespace savepoint
}  // namespace internal
}  // namespace kvlite
