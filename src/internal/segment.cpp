#include "internal/segment.h"

#include <cstring>
#include <memory>
#include <thread>

namespace kvlite {
namespace internal {

namespace {
// log2 for power-of-2 values (C++17 compatible).
uint16_t log2_pow2(uint16_t v) {
    uint16_t r = 0;
    while (v > 1) { v >>= 1; ++r; }
    return r;
}
}  // namespace

Segment::Segment() = default;
Segment::~Segment() = default;
Segment::Segment(Segment&&) noexcept = default;
Segment& Segment::operator=(Segment&&) noexcept = default;

// --- Helpers ---

std::string Segment::partitionPath(const std::string& base_path, uint16_t partition) {
    return base_path + "_" + std::to_string(partition) + ".data";
}

uint16_t Segment::partitionFor(uint64_t hash) const {
    if (num_partitions_ == 1) return 0;
    return static_cast<uint16_t>(hash >> (64 - partition_bits_));
}

// --- Lifecycle ---

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
        Status s = partitions_[i].log_file.create(partitionPath(path, i),
                                                    /*sync=*/false, buffered);
        if (!s.ok()) {
            // Clean up already-created files.
            for (uint16_t j = 0; j < i; ++j) {
                partitions_[j].log_file.close();
            }
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

    // Read partition 0's footer to discover num_partitions.
    LogFile probe;
    Status s = probe.open(partitionPath(path, 0));
    if (!s.ok()) return s;

    uint64_t file_size = probe.size();
    if (file_size < kFooterSize) {
        probe.close();
        return Status::Corruption("Segment: file too small for footer");
    }

    uint8_t footer[kFooterSize];
    s = probe.readAt(file_size - kFooterSize, footer, kFooterSize);
    if (!s.ok()) { probe.close(); return s; }

    uint32_t id;
    uint16_t num_parts;
    uint32_t magic;
    std::memcpy(&id, footer, 4);
    std::memcpy(&num_parts, footer + 4, 2);
    std::memcpy(&magic, footer + kFooterSize - 4, 4);

    if (magic != kFooterMagic) {
        probe.close();
        return Status::Corruption("Segment: bad footer magic");
    }
    if (num_parts == 0 || (num_parts & (num_parts - 1)) != 0) {
        probe.close();
        return Status::Corruption("Segment: invalid num_partitions in footer");
    }

    probe.close();

    // Now open all partitions.
    id_ = id;
    num_partitions_ = num_parts;
    partition_bits_ = (num_parts == 1) ? 0 : log2_pow2(num_parts);
    base_path_ = path;
    partitions_.resize(num_parts);

    for (uint16_t i = 0; i < num_parts; ++i) {
        auto& p = partitions_[i];
        s = p.log_file.open(partitionPath(path, i));
        if (!s.ok()) {
            for (uint16_t j = 0; j < i; ++j) partitions_[j].log_file.close();
            partitions_.clear();
            return Status::IOError("Failed to open partition " + std::to_string(i) +
                                   ": " + s.message());
        }

        uint64_t psize = p.log_file.size();
        if (psize < kFooterSize) {
            close();
            return Status::Corruption("Segment: partition too small for footer");
        }

        uint8_t pfooter[kFooterSize];
        s = p.log_file.readAt(psize - kFooterSize, pfooter, kFooterSize);
        if (!s.ok()) { close(); return s; }

        uint64_t index_offset, lineage_offset;
        std::memcpy(&index_offset, pfooter + 6, 8);
        std::memcpy(&lineage_offset, pfooter + 14, 8);

        s = p.index.readFrom(p.log_file, index_offset);
        if (!s.ok()) { close(); return s; }

        p.data_size = index_offset;
        p.lineage_offset = lineage_offset;
    }

    state_ = State::kReadable;
    return Status::OK();
}

Status Segment::sealPartition(uint16_t pi) {
    auto& p = partitions_[pi];

    // Finalize streaming index build if appendEntry was used.
    if (p.batch_mode) {
        p.index.endBatch();
        p.batch_mode = false;
    }

    // Record where data ends / index begins.
    p.data_size = p.log_file.size();

    // Append serialized SegmentIndex.
    Status s = p.index.writeTo(p.log_file);
    if (!s.ok()) return s;

    // Append lineage section.
    s = writeLineage(p, lineage_type_, kLineageMagic);
    if (!s.ok()) return s;

    // Append footer: [segment_id:4][num_partitions:2][index_offset:8][lineage_offset:8][magic:4]
    uint8_t footer[kFooterSize];
    std::memcpy(footer, &id_, 4);
    std::memcpy(footer + 4, &num_partitions_, 2);
    std::memcpy(footer + 6, &p.data_size, 8);
    std::memcpy(footer + 14, &p.lineage_offset, 8);
    uint32_t magic = kFooterMagic;
    std::memcpy(footer + 22, &magic, 4);

    uint64_t footer_offset;
    s = p.log_file.append(footer, kFooterSize, footer_offset);
    if (!s.ok()) return s;

    s = p.log_file.flushBuffer();
    if (!s.ok()) return s;

    return p.log_file.sync();
}

Status Segment::seal() {
    if (state_ != State::kWriting) {
        return Status::InvalidArgument("Segment: seal requires Writing state");
    }

    if (num_partitions_ == 1) {
        Status s = sealPartition(0);
        if (!s.ok()) return s;
    } else {
        // Seal partitions in parallel.
        std::vector<Status> results(num_partitions_);
        std::vector<std::thread> threads;
        threads.reserve(num_partitions_);
        for (uint16_t i = 0; i < num_partitions_; ++i) {
            threads.emplace_back([this, i, &results] {
                results[i] = sealPartition(i);
            });
        }
        for (auto& t : threads) t.join();
        for (const auto& s : results) {
            if (!s.ok()) return s;
        }
    }

    state_ = State::kReadable;
    return Status::OK();
}

Status Segment::close() {
    if (state_ == State::kClosed) {
        return Status::OK();
    }
    for (auto& p : partitions_) {
        p.log_file.close();
    }
    state_ = State::kClosed;
    return Status::OK();
}

// --- Write ---

Status Segment::writeEntry(Partition& p, std::string_view key, uint64_t version,
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
    Status s = p.log_file.append(buf, entry_size, offset);
    if (!s.ok()) return s;

    entry_offset = offset;
    p.data_size = offset + entry_size;
    return Status::OK();
}

Status Segment::put(std::string_view key, uint64_t version,
                    std::string_view value, bool tombstone,
                    uint64_t hash) {
    if (state_ != State::kWriting) {
        return Status::InvalidArgument("Segment: put requires Writing state");
    }
    if (key.size() > LogEntry::kMaxKeyLen) {
        return Status::InvalidArgument("Segment: key too long");
    }

    auto& p = partitions_[partitionFor(hash)];
    uint64_t offset;
    Status s = writeEntry(p, key, version, value, tombstone, offset);
    if (!s.ok()) return s;

    PackedVersion pv(version, tombstone);
    p.index.put(hash, static_cast<uint32_t>(offset), pv.data);
    addLineageEntry(p, hash, pv.data);
    return Status::OK();
}

Status Segment::appendEntry(std::string_view key, uint64_t version,
                             std::string_view value, bool tombstone,
                             uint64_t hash, uint64_t& entry_offset) {
    if (state_ != State::kWriting) {
        return Status::InvalidArgument("Segment: appendEntry requires Writing state");
    }
    if (key.size() > LogEntry::kMaxKeyLen) {
        return Status::InvalidArgument("Segment: appendEntry key too long");
    }

    auto& p = partitions_[partitionFor(hash)];
    Status s = writeEntry(p, key, version, value, tombstone, entry_offset);
    if (!s.ok()) return s;

    PackedVersion pv(version, tombstone);
    p.index.addBatchEntry(hash, pv.data, static_cast<uint32_t>(entry_offset));
    p.batch_mode = true;
    addLineageEntry(p, hash, pv.data);
    return Status::OK();
}

Status Segment::appendRawEntry(const void* payload, size_t payload_len,
                                uint64_t hash, uint64_t& entry_offset) {
    if (state_ != State::kWriting) {
        return Status::InvalidArgument("Segment: appendRawEntry requires Writing state");
    }

    auto& p = partitions_[partitionFor(hash)];
    uint32_t checksum = crc32(payload, payload_len);

    uint64_t offset;
    Status s = p.log_file.append(payload, payload_len, offset);
    if (!s.ok()) return s;

    uint64_t crc_offset;
    s = p.log_file.append(&checksum, sizeof(checksum), crc_offset);
    if (!s.ok()) return s;

    entry_offset = offset;
    p.data_size = offset + payload_len + sizeof(checksum);

    uint64_t packed_ver;
    std::memcpy(&packed_ver, payload, 8);
    p.index.addBatchEntry(hash, packed_ver, static_cast<uint32_t>(offset));
    p.batch_mode = true;
    addLineageEntry(p, hash, packed_ver);
    return Status::OK();
}

// --- Read ---

Status Segment::readEntry(uint16_t partition, uint64_t offset, LogEntry& entry) const {
    const auto& p = partitions_[partition];
    static constexpr size_t kReadAhead = 4096;
    uint8_t stack_buf[kReadAhead];

    size_t avail = (offset < p.data_size) ? static_cast<size_t>(p.data_size - offset) : 0;
    size_t first_read = (avail < kReadAhead) ? avail : kReadAhead;
    if (first_read < LogEntry::kHeaderSize + LogEntry::kChecksumSize) {
        return Status::Corruption("Segment: truncated entry at offset " +
                                  std::to_string(offset));
    }
    Status s = p.log_file.readAt(offset, stack_buf, first_read);
    if (!s.ok()) return s;

    uint64_t packed_ver;
    uint16_t kl;
    uint32_t vl;
    std::memcpy(&packed_ver, stack_buf, 8);
    std::memcpy(&kl, stack_buf + 8, 2);
    std::memcpy(&vl, stack_buf + 10, 4);

    size_t entry_size = LogEntry::kHeaderSize + kl + vl + LogEntry::kChecksumSize;

    uint8_t* buf = stack_buf;
    std::unique_ptr<uint8_t[]> heap_buf;
    if (entry_size > first_read) {
        heap_buf = std::make_unique<uint8_t[]>(entry_size);
        buf = heap_buf.get();
        s = p.log_file.readAt(offset, buf, entry_size);
        if (!s.ok()) return s;
    }

    size_t payload_len = LogEntry::kHeaderSize + kl + vl;
    uint32_t stored_crc;
    std::memcpy(&stored_crc, buf + payload_len, 4);
    uint32_t computed_crc = crc32(buf, payload_len);
    if (stored_crc != computed_crc) {
        return Status::Corruption("Segment: CRC mismatch at offset " +
                                  std::to_string(offset));
    }

    entry.pv = PackedVersion(packed_ver);
    entry.key.assign(reinterpret_cast<const char*>(buf + LogEntry::kHeaderSize), kl);
    entry.value.assign(reinterpret_cast<const char*>(buf + LogEntry::kHeaderSize + kl), vl);
    return Status::OK();
}

Status Segment::getLatest(uint64_t hash, LogEntry& entry) const {
    if (state_ != State::kReadable) {
        return Status::InvalidArgument("Segment: getLatest requires Readable state");
    }
    uint16_t pi = partitionFor(hash);
    uint32_t offset;
    uint64_t packed_version;
    if (!partitions_[pi].index.getLatest(hash, offset, packed_version)) {
        return Status::NotFound("key not found");
    }
    return readEntry(pi, offset, entry);
}

Status Segment::get(uint64_t hash, std::vector<LogEntry>& entries) const {
    if (state_ != State::kReadable) {
        return Status::InvalidArgument("Segment: get requires Readable state");
    }
    uint16_t pi = partitionFor(hash);
    std::vector<uint32_t> offsets;
    std::vector<uint64_t> packed_versions;
    if (!partitions_[pi].index.get(hash, offsets, packed_versions)) {
        return Status::NotFound("key not found");
    }
    entries.clear();
    entries.reserve(offsets.size());
    for (size_t i = 0; i < offsets.size(); ++i) {
        LogEntry e;
        Status s = readEntry(pi, offsets[i], e);
        if (!s.ok()) return s;
        entries.push_back(std::move(e));
    }
    return Status::OK();
}

Status Segment::get(uint64_t hash, uint64_t upper_bound,
                    LogEntry& entry) const {
    if (state_ != State::kReadable) {
        return Status::InvalidArgument("Segment: get requires Readable state");
    }
    uint16_t pi = partitionFor(hash);
    uint64_t offset, packed_version;
    if (!partitions_[pi].index.get(hash, upper_bound, offset, packed_version)) {
        return Status::NotFound("key not found");
    }
    return readEntry(pi, offset, entry);
}

bool Segment::contains(uint64_t hash) const {
    if (state_ != State::kReadable) return false;
    return partitions_[partitionFor(hash)].index.contains(hash);
}

// --- Lineage ---

void Segment::setLineageType(LineageType type) {
    lineage_type_ = type;
}

void Segment::addLineageEntry(Partition& p, uint64_t hkey, uint64_t packed_version) {
    size_t off = p.lineage_buf.size();
    p.lineage_buf.resize(off + 16);
    uint8_t* ptr = p.lineage_buf.data() + off;
    std::memcpy(ptr, &hkey, 8);
    std::memcpy(ptr + 8, &packed_version, 8);
    ++p.lineage_entry_count;
}

void Segment::addLineageElimination(uint64_t hkey, uint64_t packed_version,
                                     uint32_t old_segment_id) {
    // Route elimination to the partition that owns this hash.
    auto& p = partitions_[partitionFor(hkey)];
    size_t off = p.lineage_buf.size();
    p.lineage_buf.resize(off + 20);
    uint8_t* ptr = p.lineage_buf.data() + off;
    std::memcpy(ptr, &hkey, 8);
    std::memcpy(ptr + 8, &packed_version, 8);
    std::memcpy(ptr + 16, &old_segment_id, 4);
    ++p.lineage_elimination_count;
}

Status Segment::writeLineage(Partition& p, LineageType type, uint32_t lineage_magic) {
    p.lineage_offset = p.log_file.size();

    constexpr size_t kHdrSize = 13;
    uint8_t hdr[kHdrSize];
    std::memcpy(hdr, &lineage_magic, 4);
    hdr[4] = static_cast<uint8_t>(type);
    std::memcpy(hdr + 5, &p.lineage_entry_count, 4);
    std::memcpy(hdr + 9, &p.lineage_elimination_count, 4);

    uint32_t crc_state = updateCrc32(0xFFFFFFFFu, hdr, kHdrSize);
    if (!p.lineage_buf.empty()) {
        crc_state = updateCrc32(crc_state, p.lineage_buf.data(), p.lineage_buf.size());
    }
    uint32_t checksum = finalizeCrc32(crc_state);

    uint64_t off;
    Status s = p.log_file.append(hdr, kHdrSize, off);
    if (!s.ok()) return s;

    if (!p.lineage_buf.empty()) {
        s = p.log_file.append(p.lineage_buf.data(), p.lineage_buf.size(), off);
        if (!s.ok()) return s;
    }

    return p.log_file.append(&checksum, sizeof(checksum), off);
}

Status Segment::readLineage(Lineage& lineage) const {
    if (state_ != State::kReadable) {
        return Status::InvalidArgument("Segment: readLineage requires Readable state");
    }

    lineage.type = lineage_type_;
    lineage.entries.clear();
    lineage.eliminations.clear();

    for (uint16_t i = 0; i < num_partitions_; ++i) {
        Lineage part_lin;
        Status s = readPartitionLineage(partitions_[i], part_lin);
        if (!s.ok()) return s;
        if (i == 0) lineage.type = part_lin.type;
        lineage.entries.insert(lineage.entries.end(),
                               part_lin.entries.begin(), part_lin.entries.end());
        lineage.eliminations.insert(lineage.eliminations.end(),
                                    part_lin.eliminations.begin(),
                                    part_lin.eliminations.end());
    }
    return Status::OK();
}

Status Segment::readPartitionLineage(const Partition& p, Lineage& lineage) const {
    if (p.lineage_offset == 0) {
        lineage.type = LineageType::kFlush;
        lineage.entries.clear();
        lineage.eliminations.clear();
        return Status::OK();
    }

    constexpr size_t kHdrSize = 13;
    uint8_t hdr[kHdrSize];
    Status s = p.log_file.readAt(p.lineage_offset, hdr, kHdrSize);
    if (!s.ok()) return s;

    uint32_t magic;
    std::memcpy(&magic, hdr, 4);
    if (magic != kLineageMagic) {
        return Status::Corruption("Segment: bad lineage magic");
    }

    lineage.type = static_cast<LineageType>(hdr[4]);
    uint32_t entry_count, elim_count;
    std::memcpy(&entry_count, hdr + 5, 4);
    std::memcpy(&elim_count, hdr + 9, 4);

    size_t entries_bytes = static_cast<size_t>(entry_count) * 16;
    size_t elim_bytes = static_cast<size_t>(elim_count) * 20;
    size_t payload_size = kHdrSize + entries_bytes + elim_bytes;
    size_t total_size = payload_size + 4;

    std::vector<uint8_t> buf(total_size);
    s = p.log_file.readAt(p.lineage_offset, buf.data(), total_size);
    if (!s.ok()) return s;

    uint32_t stored_crc;
    std::memcpy(&stored_crc, buf.data() + payload_size, 4);
    if (crc32(buf.data(), payload_size) != stored_crc) {
        return Status::ChecksumMismatch("Segment: lineage checksum mismatch");
    }

    const uint8_t* ptr = buf.data() + kHdrSize;

    lineage.entries.resize(entry_count);
    for (uint32_t i = 0; i < entry_count; ++i) {
        auto& e = lineage.entries[i];
        std::memcpy(&e.hkey, ptr, 8);
        std::memcpy(&e.packed_version, ptr + 8, 8);
        e.old_segment_id = 0;
        ptr += 16;
    }

    lineage.eliminations.resize(elim_count);
    for (uint32_t i = 0; i < elim_count; ++i) {
        auto& e = lineage.eliminations[i];
        std::memcpy(&e.hkey, ptr, 8);
        std::memcpy(&e.packed_version, ptr + 8, 8);
        std::memcpy(&e.old_segment_id, ptr + 16, 4);
        ptr += 20;
    }

    return Status::OK();
}

// --- Stats ---

uint64_t Segment::dataSize() const {
    uint64_t total = 0;
    for (const auto& p : partitions_) total += p.data_size;
    return total;
}

uint64_t Segment::dataSize(uint16_t partition) const {
    return partitions_[partition].data_size;
}

size_t Segment::keyCount() const {
    size_t total = 0;
    for (const auto& p : partitions_) total += p.index.keyCount();
    return total;
}

size_t Segment::entryCount() const {
    size_t total = 0;
    for (const auto& p : partitions_) total += p.index.entryCount();
    return total;
}

uint64_t Segment::siEncodeCount() const {
    uint64_t total = 0;
    for (const auto& p : partitions_) total += p.index.encodeCount();
    return total;
}

uint64_t Segment::siEncodeTotalNs() const {
    uint64_t total = 0;
    for (const auto& p : partitions_) total += p.index.encodeTotalNs();
    return total;
}

uint64_t Segment::siDecodeCount() const {
    uint64_t total = 0;
    for (const auto& p : partitions_) total += p.index.decodeCount();
    return total;
}

uint64_t Segment::siDecodeTotalNs() const {
    uint64_t total = 0;
    for (const auto& p : partitions_) total += p.index.decodeTotalNs();
    return total;
}

}  // namespace internal
}  // namespace kvlite
