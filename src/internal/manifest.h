#ifndef KVLITE_INTERNAL_MANIFEST_H
#define KVLITE_INTERNAL_MANIFEST_H

#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <vector>

#include "kvlite/status.h"

namespace kvlite {
namespace internal {

// Manifest: Append-only changelog for DB-level metadata.
//
// Each record is a CRC-protected key-value mutation. On recovery the log is
// replayed to rebuild an in-memory std::map. compact() atomically rewrites
// the full state as a snapshot.
//
// File layout:
//   [magic:4 = "MNFT"] [format_version:4 = 1]     -- header (8 bytes)
//   [record 0] [record 1] ...                      -- records
//
// Record format:
//   [record_len:4][type:1][key_len:2][value_len:4][key][value][crc32:4]
//   record_len = 11 + key_len + value_len
//   crc32 covers bytes from type through value (excludes record_len and crc32)
//
// Thread-safety: All public methods are thread-safe (guarded by mutex_).
class Manifest {
public:
    Manifest();
    ~Manifest();

    Manifest(const Manifest&) = delete;
    Manifest& operator=(const Manifest&) = delete;

    // --- Lifecycle ---

    // Create a fresh manifest at the given db_path.
    Status create(const std::string& db_path);

    // Open an existing manifest at the given db_path.
    // Returns NotFound if no MANIFEST file exists.
    Status open(const std::string& db_path);

    Status close();
    bool isOpen() const;

    // --- Read ---

    bool get(const std::string& key, std::string& value) const;
    std::vector<std::string> getKeysWithPrefix(const std::string& prefix) const;

    // --- Write (append record + update in-memory map) ---

    Status set(const std::string& key, const std::string& value);
    Status remove(const std::string& key);
    Status sync();

    // --- Maintenance ---

    Status compact();

private:
    enum RecordType : uint8_t {
        kSet = 1,
        kDelete = 2,
    };

    Status recover();
    Status writeHeader(int fd);
    Status validateHeader(int fd);
    Status appendRecord(RecordType type, const std::string& key,
                        const std::string& value);

    std::string manifestPath() const;
    std::string manifestTmpPath() const;

    std::string db_path_;
    int fd_ = -1;
    bool is_open_ = false;
    std::map<std::string, std::string> state_;
    mutable std::mutex mutex_;

    static constexpr char kMagic[4] = {'M', 'N', 'F', 'T'};
    static constexpr uint32_t kFormatVersion = 1;
    static constexpr size_t kHeaderSize = 8;
    static constexpr size_t kRecordHeaderSize = 7;   // type(1) + key_len(2) + value_len(4)
    static constexpr size_t kRecordChecksumSize = 4;
    static constexpr size_t kRecordLenSize = 4;
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_MANIFEST_H
