#ifndef KVLITE_INTERNAL_LOG_FILE_H
#define KVLITE_INTERNAL_LOG_FILE_H

#include <cstdint>
#include <string>
#include <memory>
#include <mutex>

#include "kvlite/status.h"
#include "log_entry.h"

namespace kvlite {
namespace internal {

// A single append-only log file storing key-value entries.
//
// File naming: log_NNNNNNNN.data (e.g., log_00000001.data)
// Each log file has a corresponding L2 index: log_NNNNNNNN.idx
//
// Thread-safety: append() is thread-safe, read() is thread-safe for
// concurrent reads but not concurrent with writes to same region.
class LogFile {
public:
    LogFile();
    ~LogFile();

    // Non-copyable, movable
    LogFile(const LogFile&) = delete;
    LogFile& operator=(const LogFile&) = delete;
    LogFile(LogFile&& other) noexcept;
    LogFile& operator=(LogFile&& other) noexcept;

    // Open an existing log file for reading and appending
    Status open(const std::string& path);

    // Create a new log file
    Status create(const std::string& path);

    // Close the file
    Status close();

    // Append an entry to the log file
    // Returns the byte offset where the entry was written
    Status append(const LogEntry& entry, uint64_t& offset);

    // Read an entry at the given offset
    Status read(uint64_t offset, LogEntry& entry);

    // Sync data to disk (fdatasync)
    Status sync();

    // Get current file size
    uint64_t size() const;

    // Get file ID extracted from filename
    uint32_t fileId() const { return file_id_; }

    // Check if file is open
    bool isOpen() const { return fd_ >= 0; }

    // Get the file path
    const std::string& path() const { return path_; }

    // Static helpers
    static std::string makeDataPath(const std::string& dir, uint32_t file_id);
    static std::string makeIndexPath(const std::string& dir, uint32_t file_id);
    static uint32_t parseFileId(const std::string& filename);

private:
    Status writeEntry(const LogEntry& entry, uint64_t& offset);
    Status readEntry(uint64_t offset, LogEntry& entry);
    uint32_t computeChecksum(const void* data, size_t len);

    int fd_ = -1;
    std::string path_;
    uint32_t file_id_ = 0;
    uint64_t size_ = 0;
    mutable std::mutex mutex_;  // Protects append operations
};

// Manages multiple log files.
// Handles creation of new log files when current one reaches size limit.
class LogManager {
public:
    explicit LogManager(const std::string& db_path, uint64_t max_file_size = 1ULL << 30);
    ~LogManager();

    // Initialize: scan directory for existing log files
    Status init();

    // Append entry, automatically creating new file if needed
    // Returns file_id and offset where entry was written
    Status append(const LogEntry& entry, uint32_t& file_id, uint64_t& offset);

    // Read entry from specific file at offset
    Status read(uint32_t file_id, uint64_t offset, LogEntry& entry);

    // Force creation of a new log file (e.g., for flush)
    Status rotateLogFile();

    // Sync current log file
    Status sync();

    // Get list of all file IDs
    std::vector<uint32_t> getFileIds() const;

    // Get current (active) file ID
    uint32_t currentFileId() const;

    // Get total size of all log files
    uint64_t totalSize() const;

    // Get number of log files
    size_t fileCount() const;

    // Delete a log file (after GC)
    Status deleteFile(uint32_t file_id);

    // Get the database directory path
    const std::string& dbPath() const { return db_path_; }

private:
    Status openOrCreateCurrentFile();
    Status loadExistingFiles();
    uint32_t allocateFileId();

    std::string db_path_;
    uint64_t max_file_size_;
    uint32_t next_file_id_ = 1;

    std::unique_ptr<LogFile> current_file_;
    std::unordered_map<uint32_t, std::unique_ptr<LogFile>> files_;
    mutable std::mutex mutex_;
};

} // namespace internal
} // namespace kvlite

#endif // KVLITE_INTERNAL_LOG_FILE_H
