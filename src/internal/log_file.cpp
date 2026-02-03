#include "internal/log_file.h"

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>

namespace kvlite {
namespace internal {

LogFile::LogFile() = default;

LogFile::~LogFile() {
    if (isOpen()) {
        close();
    }
}

LogFile::LogFile(LogFile&& other) noexcept
    : fd_(other.fd_),
      path_(std::move(other.path_)),
      size_(other.size_) {
    other.fd_ = -1;
    other.size_ = 0;
}

LogFile& LogFile::operator=(LogFile&& other) noexcept {
    if (this != &other) {
        if (isOpen()) {
            close();
        }
        fd_ = other.fd_;
        path_ = std::move(other.path_);
        size_ = other.size_;
        other.fd_ = -1;
        other.size_ = 0;
    }
    return *this;
}

Status LogFile::open(const std::string& path) {
    if (isOpen()) {
        return Status::IOError("file already open: " + path_);
    }

    int fd = ::open(path.c_str(), O_RDWR);
    if (fd < 0) {
        return Status::IOError("open failed: " + path + ": " + std::strerror(errno));
    }

    off_t end = ::lseek(fd, 0, SEEK_END);
    if (end < 0) {
        ::close(fd);
        return Status::IOError("lseek failed: " + path + ": " + std::strerror(errno));
    }

    fd_ = fd;
    path_ = path;
    size_ = static_cast<uint64_t>(end);
    return Status::OK();
}

Status LogFile::create(const std::string& path) {
    if (isOpen()) {
        return Status::IOError("file already open: " + path_);
    }

    int fd = ::open(path.c_str(), O_CREAT | O_TRUNC | O_RDWR, 0644);
    if (fd < 0) {
        return Status::IOError("create failed: " + path + ": " + std::strerror(errno));
    }

    fd_ = fd;
    path_ = path;
    size_ = 0;
    return Status::OK();
}

Status LogFile::close() {
    if (!isOpen()) {
        return Status::IOError("file not open");
    }

    int ret = ::close(fd_);
    fd_ = -1;
    path_.clear();
    size_ = 0;

    if (ret < 0) {
        return Status::IOError("close failed: " + std::string(std::strerror(errno)));
    }
    return Status::OK();
}

Status LogFile::append(const void* data, size_t len, uint64_t& offset) {
    if (!isOpen()) {
        return Status::IOError("file not open");
    }

    std::lock_guard<std::mutex> lock(mutex_);

    offset = size_;
    const char* ptr = static_cast<const char*>(data);
    size_t remaining = len;

    while (remaining > 0) {
        ssize_t written = ::write(fd_, ptr, remaining);
        if (written < 0) {
            if (errno == EINTR) continue;
            return Status::IOError("write failed: " + path_ + ": " + std::strerror(errno));
        }
        ptr += written;
        remaining -= static_cast<size_t>(written);
    }

    size_ += len;
    return Status::OK();
}

Status LogFile::readAt(uint64_t offset, void* buf, size_t len) {
    if (!isOpen()) {
        return Status::IOError("file not open");
    }

    char* ptr = static_cast<char*>(buf);
    size_t remaining = len;
    uint64_t pos = offset;

    while (remaining > 0) {
        ssize_t n = ::pread(fd_, ptr, remaining, static_cast<off_t>(pos));
        if (n < 0) {
            if (errno == EINTR) continue;
            return Status::IOError("pread failed: " + path_ + ": " + std::strerror(errno));
        }
        if (n == 0) {
            return Status::IOError("pread: unexpected EOF at offset " +
                                   std::to_string(pos) + " in " + path_);
        }
        ptr += n;
        remaining -= static_cast<size_t>(n);
        pos += static_cast<uint64_t>(n);
    }

    return Status::OK();
}

Status LogFile::sync() {
    if (!isOpen()) {
        return Status::IOError("file not open");
    }

    if (::fdatasync(fd_) < 0) {
        return Status::IOError("fdatasync failed: " + path_ + ": " + std::strerror(errno));
    }
    return Status::OK();
}

uint64_t LogFile::size() const {
    return size_;
}

// Static helpers

std::string LogFile::makeDataPath(const std::string& dir, uint32_t file_id) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "log_%08u.data", file_id);
    return dir + "/" + buf;
}

std::string LogFile::makeIndexPath(const std::string& dir, uint32_t file_id) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "log_%08u.idx", file_id);
    return dir + "/" + buf;
}

uint32_t LogFile::parseFileId(const std::string& filename) {
    // Expect format: log_NNNNNNNN.data or log_NNNNNNNN.idx
    uint32_t id = 0;
    if (std::sscanf(filename.c_str(), "log_%08u", &id) == 1) {
        return id;
    }
    return 0;
}

} // namespace internal
} // namespace kvlite
