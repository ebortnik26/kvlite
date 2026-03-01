#ifndef KVLITE_INTERNAL_MANIFEST_H
#define KVLITE_INTERNAL_MANIFEST_H

#include <cstdint>
#include <map>
#include <mutex>
#include <string>

#include "kvlite/status.h"

namespace kvlite {
namespace internal {

// All manifest keys, centralized for discoverability.
enum class ManifestKey {
    kVmNextVersionBlock,   // "vm.next_version_block"
    kSegmentsMinSegId,     // "segments.min_seg_id"
    kSegmentsMaxSegId,     // "segments.max_seg_id"
    kGiWalMinFileId,       // "gi.wal.min_file_id"
    kGiWalMaxFileId,       // "gi.wal.max_file_id"
    kGiSavepointMaxVersion, // "gi.savepoint.max_version"
};

const char* manifestKeyStr(ManifestKey key);

// Manifest: persistent key-value map for DB-level metadata.
//
// Conceptually has two sections: Data (one entry per key) and Log (appended
// mutations). set() appends to the log. get() is served from the in-memory
// map (log supersedes data). compact() replays the log into data, creates a
// new file, and switches to it atomically.
//
// open() calls compact() after recovery. DB::close() calls compact() before
// close(). Every set() is followed by fdatasync.
//
// File layout:
//   [magic:4 = "MNFT"] [format_version:4 = 1]     -- header (8 bytes)
//   [record 0] [record 1] ...                      -- records (data, then log)
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

    // --- Read (served from in-memory map) ---

    bool get(const std::string& key, std::string& value) const;
    bool get(ManifestKey key, std::string& value) const;

    // --- Write (append record + fdatasync + update in-memory map) ---

    Status set(const std::string& key, const std::string& value);
    Status set(ManifestKey key, const std::string& value);

    // --- Maintenance ---

    Status compact();

private:
    static constexpr uint8_t kRecordTypeSet = 1;

    Status recover();
    Status compactUnlocked();
    Status writeHeader(int fd);
    Status validateHeader(int fd);
    Status appendRecord(const std::string& key, const std::string& value);

    Status fsyncDir(const std::string& dir_path);
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
