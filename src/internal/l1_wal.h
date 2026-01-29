#ifndef KVLITE_INTERNAL_L1_WAL_H
#define KVLITE_INTERNAL_L1_WAL_H

#include <cstdint>
#include <string>
#include <mutex>

#include "kvlite/status.h"
#include "l1_index.h"

namespace kvlite {
namespace internal {

// WAL operation types for L1 index
enum class WalOp : uint8_t {
    kPut = 1,     // Add entry: key, version, file_id
    kRemove = 2,  // Remove key (all versions) - used during GC
};

// Write-Ahead Log for L1 Index.
//
// The WAL records all changes to the L1 index as append-only entries.
// On recovery, the WAL is replayed on top of the last snapshot to
// reconstruct the L1 index.
//
// WAL entry format:
// ┌────┬─────────┬─────────┬─────┬─────────┬──────────┐
// │ op │ version │ file_id │ len │   key   │ checksum │
// │ 1  │    8    │    4    │  4  │   var   │    4     │
// └────┴─────────┴─────────┴─────┴─────────┴──────────┘
//
// Thread-safety: All operations are thread-safe.
class L1WAL {
public:
    L1WAL();
    ~L1WAL();

    // Open or create WAL file
    Status open(const std::string& path);

    // Close WAL file
    Status close();

    // Append a put operation
    Status appendPut(const std::string& key, uint32_t file_id);

    // Append a remove operation (for GC)
    Status appendRemove(const std::string& key);

    // Sync WAL to disk
    Status sync();

    // Replay WAL entries into an L1 index
    // This is called during recovery to rebuild the index
    Status replay(L1Index& index);

    // Truncate WAL (called after successful snapshot)
    Status truncate();

    // Get current WAL size
    uint64_t size() const;

    // Get number of entries in WAL
    uint64_t entryCount() const { return entry_count_; }

    // Check if WAL is open
    bool isOpen() const { return fd_ >= 0; }

    // WAL file path
    static std::string makePath(const std::string& db_path);

private:
    Status writeEntry(WalOp op, const std::string& key,
                      uint64_t version, uint32_t file_id);
    Status readEntry(WalOp& op, std::string& key,
                     uint64_t& version, uint32_t& file_id);

    int fd_ = -1;
    std::string path_;
    uint64_t entry_count_ = 0;
    mutable std::mutex mutex_;
};

} // namespace internal
} // namespace kvlite

#endif // KVLITE_INTERNAL_L1_WAL_H
