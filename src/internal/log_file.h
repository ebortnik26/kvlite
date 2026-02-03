#ifndef KVLITE_INTERNAL_LOG_FILE_H
#define KVLITE_INTERNAL_LOG_FILE_H

#include <cstdint>
#include <string>
#include <mutex>

#include "kvlite/status.h"

namespace kvlite {
namespace internal {

// Thin file descriptor wrapper providing raw byte-level I/O.
//
// File naming: log_NNNNNNNN.data (e.g., log_00000001.data)
// Each log file has a corresponding L2 index: log_NNNNNNNN.idx
//
// Thread-safety: append() is serialized via mutex.
// readAt() uses pread and is safe for concurrent reads.
class LogFile {
public:
    LogFile();
    ~LogFile();

    // Non-copyable, movable
    LogFile(const LogFile&) = delete;
    LogFile& operator=(const LogFile&) = delete;
    LogFile(LogFile&& other) noexcept;
    LogFile& operator=(LogFile&& other) noexcept;

    // Open an existing file for reading and writing.
    Status open(const std::string& path);

    // Create a new file, truncating if it already exists.
    Status create(const std::string& path);

    // Close the file.
    Status close();

    // Append raw bytes. Returns the byte offset where data was written.
    Status append(const void* data, size_t len, uint64_t& offset);

    // Read bytes at a given offset (pread).
    Status readAt(uint64_t offset, void* buf, size_t len);

    // Sync data to disk (fdatasync).
    Status sync();

    // Current file size (bytes appended so far).
    uint64_t size() const;

    // Check if the file descriptor is open.
    bool isOpen() const { return fd_ >= 0; }

    // Get the file path.
    const std::string& path() const { return path_; }

    // Static file naming helpers.
    static std::string makeDataPath(const std::string& dir, uint32_t file_id);
    static std::string makeIndexPath(const std::string& dir, uint32_t file_id);
    static uint32_t parseFileId(const std::string& filename);

private:
    int fd_ = -1;
    std::string path_;
    uint64_t size_ = 0;
    mutable std::mutex mutex_;
};

} // namespace internal
} // namespace kvlite

#endif // KVLITE_INTERNAL_LOG_FILE_H
