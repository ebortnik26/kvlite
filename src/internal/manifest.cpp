#include "internal/manifest.h"

#include <cerrno>
#include <cstring>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "internal/crc32.h"

namespace kvlite {
namespace internal {

Manifest::Manifest() = default;

Manifest::~Manifest() {
    if (is_open_) {
        close();
    }
}

Status Manifest::create(const std::string& db_path) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (is_open_) {
        return Status::InvalidArgument("Manifest already open");
    }

    db_path_ = db_path;
    ::mkdir(db_path.c_str(), 0755);

    std::string path = manifestPath();
    int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        return Status::IOError("Failed to create MANIFEST: " + path +
                               " (" + std::strerror(errno) + ")");
    }

    fd_ = fd;
    Status s = writeHeader(fd_);
    if (!s.ok()) {
        ::close(fd_);
        fd_ = -1;
        return s;
    }

    is_open_ = true;
    return Status::OK();
}

Status Manifest::open(const std::string& db_path) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (is_open_) {
        return Status::InvalidArgument("Manifest already open");
    }

    db_path_ = db_path;

    std::string path = manifestPath();
    int fd = ::open(path.c_str(), O_RDWR, 0644);
    if (fd < 0) {
        if (errno == ENOENT) {
            return Status::NotFound("MANIFEST not found: " + path);
        }
        return Status::IOError("Failed to open MANIFEST: " + path +
                               " (" + std::strerror(errno) + ")");
    }

    fd_ = fd;

    Status s = validateHeader(fd_);
    if (!s.ok()) {
        ::close(fd_);
        fd_ = -1;
        return s;
    }

    s = recover();
    if (!s.ok()) {
        ::close(fd_);
        fd_ = -1;
        return s;
    }

    is_open_ = true;
    return Status::OK();
}

Status Manifest::close() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!is_open_) {
        return Status::OK();
    }

    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }

    state_.clear();
    is_open_ = false;
    return Status::OK();
}

bool Manifest::isOpen() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return is_open_;
}

bool Manifest::get(const std::string& key, std::string& value) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = state_.find(key);
    if (it == state_.end()) {
        return false;
    }
    value = it->second;
    return true;
}

std::vector<std::string> Manifest::getKeysWithPrefix(
    const std::string& prefix) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> result;
    // Use lower_bound to start at the prefix.
    for (auto it = state_.lower_bound(prefix); it != state_.end(); ++it) {
        if (it->first.compare(0, prefix.size(), prefix) != 0) {
            break;
        }
        result.push_back(it->first);
    }
    return result;
}

Status Manifest::set(const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lock(mutex_);
    Status s = appendRecord(kSet, key, value);
    if (!s.ok()) return s;
    state_[key] = value;
    return Status::OK();
}

Status Manifest::remove(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    Status s = appendRecord(kDelete, key, "");
    if (!s.ok()) return s;
    state_.erase(key);
    return Status::OK();
}

Status Manifest::sync() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (fd_ < 0) {
        return Status::IOError("Manifest not open");
    }
    if (::fdatasync(fd_) != 0) {
        return Status::IOError("Failed to fdatasync MANIFEST");
    }
    return Status::OK();
}

Status Manifest::compact() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!is_open_) {
        return Status::InvalidArgument("Manifest not open");
    }

    std::string tmp_path = manifestTmpPath();
    int tmp_fd = ::open(tmp_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (tmp_fd < 0) {
        return Status::IOError("Failed to create MANIFEST.tmp");
    }

    // Write header.
    {
        char header[kHeaderSize];
        std::memcpy(header, kMagic, 4);
        uint32_t ver = kFormatVersion;
        std::memcpy(header + 4, &ver, 4);
        ssize_t written = ::write(tmp_fd, header, kHeaderSize);
        if (written != static_cast<ssize_t>(kHeaderSize)) {
            ::close(tmp_fd);
            ::unlink(tmp_path.c_str());
            return Status::IOError("Failed to write MANIFEST.tmp header");
        }
    }

    // Write one SET record per in-memory KV pair.
    for (const auto& [key, value] : state_) {
        uint16_t key_len = static_cast<uint16_t>(key.size());
        uint32_t value_len = static_cast<uint32_t>(value.size());
        uint32_t record_len = kRecordHeaderSize + key_len + value_len +
                              kRecordChecksumSize;

        // Build payload: type + key_len + value_len + key + value
        size_t payload_size = kRecordHeaderSize + key_len + value_len;
        std::vector<char> payload(payload_size);
        size_t off = 0;
        payload[off++] = static_cast<char>(kSet);
        std::memcpy(payload.data() + off, &key_len, 2);
        off += 2;
        std::memcpy(payload.data() + off, &value_len, 4);
        off += 4;
        std::memcpy(payload.data() + off, key.data(), key_len);
        off += key_len;
        std::memcpy(payload.data() + off, value.data(), value_len);

        uint32_t checksum = crc32(payload.data(), payload_size);

        // Write: record_len + payload + crc32
        std::vector<char> record(kRecordLenSize + payload_size + kRecordChecksumSize);
        size_t roff = 0;
        std::memcpy(record.data() + roff, &record_len, 4);
        roff += 4;
        std::memcpy(record.data() + roff, payload.data(), payload_size);
        roff += payload_size;
        std::memcpy(record.data() + roff, &checksum, 4);

        ssize_t written = ::write(tmp_fd, record.data(), record.size());
        if (written != static_cast<ssize_t>(record.size())) {
            ::close(tmp_fd);
            ::unlink(tmp_path.c_str());
            return Status::IOError("Failed to write MANIFEST.tmp record");
        }
    }

    if (::fdatasync(tmp_fd) != 0) {
        ::close(tmp_fd);
        ::unlink(tmp_path.c_str());
        return Status::IOError("Failed to fdatasync MANIFEST.tmp");
    }
    ::close(tmp_fd);

    // Atomic rename.
    std::string path = manifestPath();
    if (::rename(tmp_path.c_str(), path.c_str()) != 0) {
        ::unlink(tmp_path.c_str());
        return Status::IOError("Failed to rename MANIFEST.tmp -> MANIFEST");
    }

    // Reopen the new file.
    ::close(fd_);
    fd_ = ::open(path.c_str(), O_RDWR, 0644);
    if (fd_ < 0) {
        is_open_ = false;
        return Status::IOError("Failed to reopen MANIFEST after compaction");
    }

    // Seek to end for future appends.
    ::lseek(fd_, 0, SEEK_END);

    return Status::OK();
}

// --- Private ---

Status Manifest::recover() {
    // Seek past the header.
    off_t pos = kHeaderSize;
    off_t file_size = ::lseek(fd_, 0, SEEK_END);
    if (file_size < 0) {
        return Status::IOError("Failed to seek in MANIFEST");
    }

    off_t last_valid = pos;

    while (pos + static_cast<off_t>(kRecordLenSize) <= file_size) {
        // Read record_len.
        uint32_t record_len = 0;
        ssize_t n = ::pread(fd_, &record_len, 4, pos);
        if (n != 4) break;

        // Sanity check: record_len must cover at least header + crc.
        if (record_len < kRecordHeaderSize + kRecordChecksumSize) break;

        // Check that the full record fits in the file.
        if (pos + kRecordLenSize + record_len > static_cast<uint64_t>(file_size)) break;

        // Read the full record body (type..value..crc).
        std::vector<char> buf(record_len);
        n = ::pread(fd_, buf.data(), record_len, pos + kRecordLenSize);
        if (n != static_cast<ssize_t>(record_len)) break;

        // Split into payload and stored CRC.
        size_t payload_size = record_len - kRecordChecksumSize;
        uint32_t stored_crc;
        std::memcpy(&stored_crc, buf.data() + payload_size, 4);

        uint32_t computed_crc = crc32(buf.data(), payload_size);
        if (stored_crc != computed_crc) break;

        // Parse payload.
        size_t off = 0;
        auto type = static_cast<RecordType>(buf[off++]);
        if (type != kSet && type != kDelete) break;

        uint16_t key_len;
        std::memcpy(&key_len, buf.data() + off, 2);
        off += 2;

        uint32_t value_len;
        std::memcpy(&value_len, buf.data() + off, 4);
        off += 4;

        // Validate lengths match record_len.
        if (kRecordHeaderSize + key_len + value_len + kRecordChecksumSize !=
            record_len) {
            break;
        }

        std::string key(buf.data() + off, key_len);
        off += key_len;
        std::string value(buf.data() + off, value_len);

        if (type == kSet) {
            state_[key] = value;
        } else {
            state_.erase(key);
        }

        pos += kRecordLenSize + record_len;
        last_valid = pos;
    }

    // Truncate to last valid record boundary.
    if (last_valid < file_size) {
        if (::ftruncate(fd_, last_valid) != 0) {
            return Status::IOError("Failed to truncate MANIFEST");
        }
    }

    // Seek to end for future appends.
    ::lseek(fd_, 0, SEEK_END);

    return Status::OK();
}

Status Manifest::writeHeader(int fd) {
    char header[kHeaderSize];
    std::memcpy(header, kMagic, 4);
    uint32_t ver = kFormatVersion;
    std::memcpy(header + 4, &ver, 4);

    ssize_t written = ::write(fd, header, kHeaderSize);
    if (written != static_cast<ssize_t>(kHeaderSize)) {
        return Status::IOError("Failed to write MANIFEST header");
    }
    return Status::OK();
}

Status Manifest::validateHeader(int fd) {
    char header[kHeaderSize];
    ssize_t n = ::pread(fd, header, kHeaderSize, 0);
    if (n != static_cast<ssize_t>(kHeaderSize)) {
        return Status::Corruption("MANIFEST header too short");
    }

    if (std::memcmp(header, kMagic, 4) != 0) {
        return Status::Corruption("Invalid MANIFEST magic");
    }

    uint32_t ver;
    std::memcpy(&ver, header + 4, 4);
    if (ver != kFormatVersion) {
        return Status::Corruption("Unsupported MANIFEST format version");
    }

    return Status::OK();
}

Status Manifest::appendRecord(RecordType type, const std::string& key,
                               const std::string& value) {
    if (fd_ < 0) {
        return Status::IOError("Manifest not open");
    }

    uint16_t key_len = static_cast<uint16_t>(key.size());
    uint32_t value_len = static_cast<uint32_t>(value.size());

    // Build payload: type + key_len + value_len + key + value
    size_t payload_size = kRecordHeaderSize + key_len + value_len;
    std::vector<char> payload(payload_size);
    size_t off = 0;
    payload[off++] = static_cast<char>(type);
    std::memcpy(payload.data() + off, &key_len, 2);
    off += 2;
    std::memcpy(payload.data() + off, &value_len, 4);
    off += 4;
    std::memcpy(payload.data() + off, key.data(), key_len);
    off += key_len;
    std::memcpy(payload.data() + off, value.data(), value_len);

    uint32_t checksum = crc32(payload.data(), payload_size);
    uint32_t record_len = static_cast<uint32_t>(payload_size + kRecordChecksumSize);

    // Write atomically: record_len + payload + crc32
    size_t total = kRecordLenSize + payload_size + kRecordChecksumSize;
    std::vector<char> record(total);
    size_t roff = 0;
    std::memcpy(record.data() + roff, &record_len, 4);
    roff += 4;
    std::memcpy(record.data() + roff, payload.data(), payload_size);
    roff += payload_size;
    std::memcpy(record.data() + roff, &checksum, 4);

    ssize_t written = ::write(fd_, record.data(), total);
    if (written != static_cast<ssize_t>(total)) {
        return Status::IOError("Failed to append MANIFEST record");
    }

    return Status::OK();
}

std::string Manifest::manifestPath() const {
    return db_path_ + "/MANIFEST";
}

std::string Manifest::manifestTmpPath() const {
    return db_path_ + "/MANIFEST.tmp";
}

}  // namespace internal
}  // namespace kvlite
