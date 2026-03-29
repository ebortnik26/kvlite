#include "internal/segment.h"

#include <condition_variable>
#include <cstring>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>

namespace kvlite {
namespace internal {

namespace {
uint16_t log2_pow2(uint16_t v) {
    uint16_t r = 0;
    while (v > 1) { v >>= 1; ++r; }
    return r;
}
}  // namespace

// ===================================================================
// FlushPool
// ===================================================================

struct FlushPool::Impl {
    std::vector<std::thread> workers;
    std::mutex mu;
    std::condition_variable cv;
    std::condition_variable done_cv;
    std::vector<std::function<void()>> tasks;
    size_t pending = 0;
    bool stop = false;

    void workerLoop() {
        for (;;) {
            std::function<void()> task;
            {
                std::unique_lock lk(mu);
                cv.wait(lk, [this] { return stop || !tasks.empty(); });
                if (stop && tasks.empty()) return;
                task = std::move(tasks.front());
                tasks.erase(tasks.begin());
            }
            task();
            {
                std::lock_guard lk(mu);
                if (--pending == 0) done_cv.notify_all();
            }
        }
    }
};

FlushPool::FlushPool(size_t num_threads) : impl_(std::make_unique<Impl>()) {
    for (size_t i = 0; i < num_threads; ++i) {
        impl_->workers.emplace_back([this] { impl_->workerLoop(); });
    }
}

FlushPool::~FlushPool() {
    {
        std::lock_guard lk(impl_->mu);
        impl_->stop = true;
    }
    impl_->cv.notify_all();
    for (auto& t : impl_->workers) t.join();
}

void FlushPool::submit(std::function<void()> fn) {
    {
        std::lock_guard lk(impl_->mu);
        ++impl_->pending;
        impl_->tasks.push_back(std::move(fn));
    }
    impl_->cv.notify_one();
}

void FlushPool::wait() {
    std::unique_lock lk(impl_->mu);
    impl_->done_cv.wait(lk, [this] { return impl_->pending == 0; });
}

// ===================================================================
// SegmentPartition
// ===================================================================

SegmentPartition::SegmentPartition() = default;
SegmentPartition::~SegmentPartition() = default;
SegmentPartition::SegmentPartition(SegmentPartition&&) noexcept = default;
SegmentPartition& SegmentPartition::operator=(SegmentPartition&&) noexcept = default;

// --- Lifecycle ---

Status SegmentPartition::create(const std::string& path, bool buffered) {
    return log_file_.create(path, /*sync=*/false, buffered, /*async_writeback=*/true);
}

Status SegmentPartition::open(const std::string& path,
                               uint32_t& out_segment_id) {
    Status s = log_file_.open(path);
    if (!s.ok()) return s;

    uint64_t file_size = log_file_.size();
    if (file_size < kFooterSize) {
        log_file_.close();
        return Status::Corruption("Segment: file too small for footer");
    }

    // Footer: [segment_id:4][index_offset:8][lineage_offset:8][magic:4]
    uint8_t footer[kFooterSize];
    s = log_file_.readAt(file_size - kFooterSize, footer, kFooterSize);
    if (!s.ok()) { log_file_.close(); return s; }

    uint64_t index_offset;
    uint32_t magic;
    std::memcpy(&out_segment_id, footer, 4);
    std::memcpy(&index_offset, footer + 4, 8);
    std::memcpy(&lineage_offset_, footer + 12, 8);
    std::memcpy(&magic, footer + 20, 4);

    if (magic != kFooterMagic) {
        log_file_.close();
        return Status::Corruption("Segment: bad footer magic");
    }

    s = index_.readFrom(log_file_, index_offset);
    if (!s.ok()) { log_file_.close(); return s; }

    data_size_ = index_offset;
    return Status::OK();
}

Status SegmentPartition::seal(uint32_t segment_id) {
    if (batch_mode_) {
        index_.endBatch();
        batch_mode_ = false;
    }

    data_size_ = log_file_.size();

    Status s = index_.writeTo(log_file_);
    if (!s.ok()) return s;

    s = writeLineageSection();
    if (!s.ok()) return s;

    // Footer: [segment_id:4][index_offset:8][lineage_offset:8][magic:4]
    uint8_t footer[kFooterSize];
    uint32_t magic = kFooterMagic;
    std::memcpy(footer, &segment_id, 4);
    std::memcpy(footer + 4, &data_size_, 8);
    std::memcpy(footer + 12, &lineage_offset_, 8);
    std::memcpy(footer + 20, &magic, 4);

    uint64_t footer_offset;
    s = log_file_.append(footer, kFooterSize, footer_offset);
    if (!s.ok()) return s;

    s = log_file_.flushBuffer();
    if (!s.ok()) return s;

    return log_file_.sync();
}

// --- Write ---

Status SegmentPartition::writeEntry(std::string_view key, uint64_t version,
                                     std::string_view value, bool tombstone,
                                     uint64_t& entry_offset) {
    PackedVersion pv(version, tombstone);
    uint64_t packed_ver = pv.data;
    uint16_t key_len = static_cast<uint16_t>(key.size());
    uint32_t val_len = static_cast<uint32_t>(value.size());
    size_t raw_key_len = key.size();
    size_t entry_size = LogEntry::kHeaderSize + raw_key_len + val_len +
                        LogEntry::kChecksumSize;

    uint8_t stack_buf[4096];
    std::unique_ptr<uint8_t[]> heap_buf;
    uint8_t* buf = stack_buf;
    if (entry_size > sizeof(stack_buf)) {
        heap_buf = std::make_unique<uint8_t[]>(entry_size);
        buf = heap_buf.get();
    }

    uint8_t* bp = buf;
    std::memcpy(bp, &packed_ver, 8); bp += 8;
    std::memcpy(bp, &key_len, 2);    bp += 2;
    std::memcpy(bp, &val_len, 4);    bp += 4;
    std::memcpy(bp, key.data(), raw_key_len);   bp += raw_key_len;
    std::memcpy(bp, value.data(), val_len);  bp += val_len;

    size_t payload_len = LogEntry::kHeaderSize + raw_key_len + val_len;
    uint32_t checksum = crc32(buf, payload_len);
    std::memcpy(bp, &checksum, 4);

    uint64_t offset;
    Status s = log_file_.append(buf, entry_size, offset);
    if (!s.ok()) return s;

    entry_offset = offset;
    data_size_ = offset + entry_size;
    return Status::OK();
}

Status SegmentPartition::put(std::string_view key, uint64_t version,
                              std::string_view value, bool tombstone,
                              uint64_t hash) {
    uint64_t offset;
    Status s = writeEntry(key, version, value, tombstone, offset);
    if (!s.ok()) return s;

    PackedVersion pv(version, tombstone);
    index_.put(hash, static_cast<uint32_t>(offset), pv.data);
    if (lineage_type_ == LineageType::kFlush) addLineagePresent(hash, pv.data);
    return Status::OK();
}

Status SegmentPartition::appendEntry(std::string_view key, uint64_t version,
                                      std::string_view value, bool tombstone,
                                      uint64_t hash, uint64_t& entry_offset) {
    Status s = writeEntry(key, version, value, tombstone, entry_offset);
    if (!s.ok()) return s;

    PackedVersion pv(version, tombstone);
    index_.addBatchEntry(hash, pv.data, static_cast<uint32_t>(entry_offset));
    batch_mode_ = true;
    if (lineage_type_ == LineageType::kFlush) addLineagePresent(hash, pv.data);
    return Status::OK();
}

Status SegmentPartition::appendRawEntry(const void* payload, size_t payload_len,
                                         uint64_t hash, uint64_t& entry_offset) {
    uint32_t checksum = crc32(payload, payload_len);

    uint64_t offset;
    Status s = log_file_.append(payload, payload_len, offset);
    if (!s.ok()) return s;

    uint64_t crc_offset;
    s = log_file_.append(&checksum, sizeof(checksum), crc_offset);
    if (!s.ok()) return s;

    entry_offset = offset;
    data_size_ = offset + payload_len + sizeof(checksum);

    uint64_t packed_ver;
    std::memcpy(&packed_ver, payload, 8);
    index_.addBatchEntry(hash, packed_ver, static_cast<uint32_t>(offset));
    batch_mode_ = true;
    if (lineage_type_ == LineageType::kFlush) addLineagePresent(hash, packed_ver);
    return Status::OK();
}

// --- Read ---

// Speculative 4KB read + CRC validation. Returns a pointer to the
// validated buffer and parsed field lengths. The buffer is either
// stack_buf (if the entry fits in 4KB) or heap_buf (caller owns).
Status SegmentPartition::readRaw(uint64_t offset,
                                  uint8_t* stack_buf, size_t stack_size,
                                  std::unique_ptr<uint8_t[]>& heap_buf,
                                  const uint8_t*& out_buf,
                                  uint16_t& out_key_len,
                                  uint32_t& out_val_len) const {
    size_t avail = (offset < data_size_) ? static_cast<size_t>(data_size_ - offset) : 0;
    size_t first_read = (avail < stack_size) ? avail : stack_size;
    if (first_read < LogEntry::kHeaderSize + LogEntry::kChecksumSize) {
        return Status::Corruption("Segment: truncated entry at offset " +
                                  std::to_string(offset));
    }
    Status s = log_file_.readAt(offset, stack_buf, first_read);
    if (!s.ok()) return s;

    std::memcpy(&out_key_len, stack_buf + 8, 2);
    std::memcpy(&out_val_len, stack_buf + 10, 4);

    size_t entry_size = LogEntry::kHeaderSize + out_key_len + out_val_len +
                        LogEntry::kChecksumSize;

    uint8_t* buf = stack_buf;
    if (entry_size > first_read) {
        heap_buf = std::make_unique<uint8_t[]>(entry_size);
        buf = heap_buf.get();
        s = log_file_.readAt(offset, buf, entry_size);
        if (!s.ok()) return s;
    }

    size_t payload_len = LogEntry::kHeaderSize + out_key_len + out_val_len;
    uint32_t stored_crc;
    std::memcpy(&stored_crc, buf + payload_len, 4);
    if (crc32(buf, payload_len) != stored_crc) {
        return Status::Corruption("Segment: CRC mismatch at offset " +
                                  std::to_string(offset));
    }

    out_buf = buf;
    return Status::OK();
}

Status SegmentPartition::readEntry(uint64_t offset, LogEntry& entry) const {
    static constexpr size_t kReadAhead = 4096;
    uint8_t stack_buf[kReadAhead];
    std::unique_ptr<uint8_t[]> heap_buf;
    const uint8_t* buf;
    uint16_t kl;
    uint32_t vl;

    Status s = readRaw(offset, stack_buf, kReadAhead, heap_buf, buf, kl, vl);
    if (!s.ok()) return s;

    uint64_t packed_ver;
    std::memcpy(&packed_ver, buf, 8);
    entry.pv = PackedVersion(packed_ver);
    entry.key.assign(reinterpret_cast<const char*>(buf + LogEntry::kHeaderSize), kl);
    entry.value.assign(reinterpret_cast<const char*>(buf + LogEntry::kHeaderSize + kl), vl);
    return Status::OK();
}

Status SegmentPartition::readValue(uint64_t offset, std::string& value) const {
    static constexpr size_t kReadAhead = 4096;
    uint8_t stack_buf[kReadAhead];
    std::unique_ptr<uint8_t[]> heap_buf;
    const uint8_t* buf;
    uint16_t kl;
    uint32_t vl;

    Status s = readRaw(offset, stack_buf, kReadAhead, heap_buf, buf, kl, vl);
    if (!s.ok()) return s;

    value.assign(reinterpret_cast<const char*>(buf + LogEntry::kHeaderSize + kl), vl);
    return Status::OK();
}

Status SegmentPartition::getLatest(uint64_t hash, LogEntry& entry) const {
    uint32_t offset;
    uint64_t packed_version;
    if (!index_.getLatest(hash, offset, packed_version)) {
        return Status::NotFound("key not found");
    }
    return readEntry(offset, entry);
}

Status SegmentPartition::get(uint64_t hash, std::vector<LogEntry>& entries) const {
    std::vector<uint32_t> offsets;
    std::vector<uint64_t> packed_versions;
    if (!index_.get(hash, offsets, packed_versions)) {
        return Status::NotFound("key not found");
    }
    entries.clear();
    entries.reserve(offsets.size());
    for (size_t i = 0; i < offsets.size(); ++i) {
        LogEntry e;
        Status s = readEntry(offsets[i], e);
        if (!s.ok()) return s;
        entries.push_back(std::move(e));
    }
    return Status::OK();
}

Status SegmentPartition::get(uint64_t hash, uint64_t upper_bound,
                              LogEntry& entry) const {
    uint64_t offset, packed_version;
    if (!index_.get(hash, upper_bound, offset, packed_version)) {
        return Status::NotFound("key not found");
    }
    return readEntry(offset, entry);
}

// --- Lineage ---

static void appendRecord(std::vector<uint8_t>& buf,
                         uint64_t hkey, uint64_t packed_version,
                         uint32_t old_segment_id) {
    size_t off = buf.size();
    buf.resize(off + 20);
    uint8_t* ptr = buf.data() + off;
    std::memcpy(ptr, &hkey, 8);
    std::memcpy(ptr + 8, &packed_version, 8);
    std::memcpy(ptr + 16, &old_segment_id, 4);
}

void SegmentPartition::addLineagePresent(uint64_t hkey, uint64_t packed_version,
                                          uint32_t old_segment_id) {
    appendRecord(lineage_buf_, hkey, packed_version, old_segment_id);
    ++lineage_present_count_;
}

void SegmentPartition::addLineageDeleted(uint64_t hkey, uint64_t packed_version,
                                          uint32_t old_segment_id) {
    appendRecord(lineage_buf_, hkey, packed_version, old_segment_id);
    ++lineage_deleted_count_;
}

Status SegmentPartition::writeLineageSection() {
    lineage_offset_ = log_file_.size();

    // Header: [magic:4][type:1][present_count:4][deleted_count:4] = 13 bytes
    constexpr size_t kHdrSize = 13;
    uint8_t hdr[kHdrSize];
    uint32_t magic = kLineageMagic;
    std::memcpy(hdr, &magic, 4);
    hdr[4] = static_cast<uint8_t>(lineage_type_);
    std::memcpy(hdr + 5, &lineage_present_count_, 4);
    std::memcpy(hdr + 9, &lineage_deleted_count_, 4);

    uint32_t crc_state = updateCrc32(0xFFFFFFFFu, hdr, kHdrSize);
    if (!lineage_buf_.empty()) {
        crc_state = updateCrc32(crc_state, lineage_buf_.data(), lineage_buf_.size());
    }
    uint32_t checksum = finalizeCrc32(crc_state);

    uint64_t off;
    Status s = log_file_.append(hdr, kHdrSize, off);
    if (!s.ok()) return s;

    if (!lineage_buf_.empty()) {
        s = log_file_.append(lineage_buf_.data(), lineage_buf_.size(), off);
        if (!s.ok()) return s;
    }

    return log_file_.append(&checksum, sizeof(checksum), off);
}

Status SegmentPartition::readLineage(Lineage& lineage) const {
    if (lineage_offset_ == 0) {
        lineage.type = LineageType::kFlush;
        lineage.present.clear();
        lineage.deleted.clear();
        return Status::OK();
    }

    constexpr size_t kHdrSize = 13;
    uint8_t hdr[kHdrSize];
    Status s = log_file_.readAt(lineage_offset_, hdr, kHdrSize);
    if (!s.ok()) return s;

    uint32_t magic;
    std::memcpy(&magic, hdr, 4);
    if (magic != kLineageMagic) {
        return Status::Corruption("Segment: bad lineage magic");
    }

    lineage.type = static_cast<LineageType>(hdr[4]);
    uint32_t present_count, deleted_count;
    std::memcpy(&present_count, hdr + 5, 4);
    std::memcpy(&deleted_count, hdr + 9, 4);

    uint32_t total_records = present_count + deleted_count;
    size_t payload_size = kHdrSize + static_cast<size_t>(total_records) * 20;
    size_t total_size = payload_size + 4;

    std::vector<uint8_t> buf(total_size);
    s = log_file_.readAt(lineage_offset_, buf.data(), total_size);
    if (!s.ok()) return s;

    uint32_t stored_crc;
    std::memcpy(&stored_crc, buf.data() + payload_size, 4);
    if (crc32(buf.data(), payload_size) != stored_crc) {
        return Status::ChecksumMismatch("Segment: lineage checksum mismatch");
    }

    const uint8_t* ptr = buf.data() + kHdrSize;

    auto readRecords = [&](std::vector<Lineage::Record>& vec, uint32_t count) {
        vec.resize(count);
        for (uint32_t i = 0; i < count; ++i) {
            auto& e = vec[i];
            std::memcpy(&e.hkey, ptr, 8);
            std::memcpy(&e.packed_version, ptr + 8, 8);
            std::memcpy(&e.old_segment_id, ptr + 16, 4);
            ptr += 20;
        }
    };
    readRecords(lineage.present, present_count);
    readRecords(lineage.deleted, deleted_count);

    return Status::OK();
}

// ===================================================================
// Segment (thin coordinator)
// ===================================================================

Segment::Segment() = default;
Segment::~Segment() = default;
Segment::Segment(Segment&&) noexcept = default;
Segment& Segment::operator=(Segment&&) noexcept = default;

std::string Segment::partitionPath(const std::string& base_path, uint16_t partition) {
    return base_path + "_" + std::to_string(partition) + ".data";
}

Status Segment::create(const std::string& path, uint32_t id,
                        uint16_t num_partitions, bool buffered) {
    if (state_ != State::kClosed) {
        return Status::InvalidArgument("Segment: create requires Closed state");
    }
    if (num_partitions == 0 || (num_partitions & (num_partitions - 1)) != 0) {
        return Status::InvalidArgument("Segment: num_partitions must be a power of 2");
    }

    id_ = id;
    num_partitions_ = num_partitions;
    partition_bits_ = (num_partitions == 1) ? 0 : log2_pow2(num_partitions);
    base_path_ = path;
    partitions_.resize(num_partitions);

    for (uint16_t i = 0; i < num_partitions; ++i) {
        Status s = partitions_[i].create(partitionPath(path, i), buffered);
        if (!s.ok()) {
            for (uint16_t j = 0; j < i; ++j) partitions_[j].close();
            partitions_.clear();
            return s;
        }
    }

    state_ = State::kWriting;
    return Status::OK();
}

Status Segment::open(const std::string& path) {
    if (state_ != State::kClosed) {
        return Status::InvalidArgument("Segment: open requires Closed state");
    }

    base_path_ = path;

    // Discover partition count by probing files: _0.data, _1.data, ...
    // A segment always has at least partition 0.
    std::vector<SegmentPartition> parts;
    uint32_t id = 0;
    for (uint16_t i = 0; ; ++i) {
        std::string ppath = partitionPath(path, i);
        SegmentPartition p;
        uint32_t pid;
        Status s = p.open(ppath, pid);
        if (!s.ok()) break;  // No more partitions.
        if (i == 0) {
            id = pid;
        }
        parts.push_back(std::move(p));
    }

    if (parts.empty()) {
        return Status::IOError("Segment: no partition files found for " + path);
    }

    uint16_t num_parts = static_cast<uint16_t>(parts.size());
    if (num_parts > 1 && (num_parts & (num_parts - 1)) != 0) {
        for (auto& p : parts) p.close();
        return Status::Corruption("Segment: partition count " +
                                  std::to_string(num_parts) + " is not a power of 2");
    }

    id_ = id;
    num_partitions_ = num_parts;
    partition_bits_ = (num_parts == 1) ? 0 : log2_pow2(num_parts);
    partitions_ = std::move(parts);
    state_ = State::kReadable;
    return Status::OK();
}

Status Segment::seal(FlushPool* pool) {
    if (state_ != State::kWriting) {
        return Status::InvalidArgument("Segment: seal requires Writing state");
    }

    if (num_partitions_ == 1 || !pool) {
        for (uint16_t i = 0; i < num_partitions_; ++i) {
            Status s = partitions_[i].seal(id_);
            if (!s.ok()) return s;
        }
    } else {
        std::vector<Status> results(num_partitions_);
        for (uint16_t i = 0; i < num_partitions_; ++i) {
            pool->submit([this, i, &results] {
                results[i] = partitions_[i].seal(id_);
            });
        }
        pool->wait();
        for (const auto& s : results) {
            if (!s.ok()) return s;
        }
    }

    state_ = State::kReadable;
    return Status::OK();
}

Status Segment::close() {
    if (state_ == State::kClosed) return Status::OK();
    for (auto& p : partitions_) p.close();
    state_ = State::kClosed;
    return Status::OK();
}

Status Segment::readLineage(Lineage& lineage) const {
    if (state_ != State::kReadable) {
        return Status::InvalidArgument("Segment: readLineage requires Readable state");
    }

    lineage.present.clear();
    lineage.deleted.clear();

    for (uint16_t i = 0; i < num_partitions_; ++i) {
        Lineage part_lin;
        Status s = partitions_[i].readLineage(part_lin);
        if (!s.ok()) return s;
        if (i == 0) lineage.type = part_lin.type;
        lineage.present.insert(lineage.present.end(),
                               part_lin.present.begin(), part_lin.present.end());
        lineage.deleted.insert(lineage.deleted.end(),
                               part_lin.deleted.begin(), part_lin.deleted.end());
    }
    return Status::OK();
}

// --- Aggregate stats ---

uint64_t Segment::dataSize() const {
    uint64_t total = 0;
    for (const auto& p : partitions_) total += p.dataSize();
    return total;
}

size_t Segment::keyCount() const {
    size_t total = 0;
    for (const auto& p : partitions_) total += p.keyCount();
    return total;
}

size_t Segment::entryCount() const {
    size_t total = 0;
    for (const auto& p : partitions_) total += p.entryCount();
    return total;
}

uint64_t Segment::siEncodeCount() const {
    uint64_t total = 0;
    for (const auto& p : partitions_) total += p.siEncodeCount();
    return total;
}

uint64_t Segment::siEncodeTotalNs() const {
    uint64_t total = 0;
    for (const auto& p : partitions_) total += p.siEncodeTotalNs();
    return total;
}

uint64_t Segment::siDecodeCount() const {
    uint64_t total = 0;
    for (const auto& p : partitions_) total += p.siDecodeCount();
    return total;
}

uint64_t Segment::siDecodeTotalNs() const {
    uint64_t total = 0;
    for (const auto& p : partitions_) total += p.siDecodeTotalNs();
    return total;
}

}  // namespace internal
}  // namespace kvlite
