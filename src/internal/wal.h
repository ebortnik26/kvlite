#ifndef KVLITE_INTERNAL_WAL_H
#define KVLITE_INTERNAL_WAL_H

#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <vector>

#include "kvlite/status.h"
#include "internal/log_file.h"

namespace kvlite {
namespace internal {

// Opaque-record Write-Ahead Log with implicit transaction semantics.
//
// On-disk record format:
//
// Data record:   [body_len:4][type:1 = 0x01][payload:var][crc32:4]
// Commit record: [body_len:4][type:1 = 0x02][seq:8][crc32:4]
//
// body_len = 1 + payload_len + 4  (data record)
//          = 1 + 8 + 4 = 13       (commit record)
//
// crc32 covers: type byte + payload/seq (everything between body_len and crc32)
//
// Single implicit transaction: put() buffers data records in memory (no I/O).
// commit() appends all buffered records + a commit record to disk atomically,
// then clears the buffer. abort() discards the buffer without writing.
class WAL {
public:
    WAL();
    ~WAL();

    WAL(WAL&& other) noexcept;
    WAL& operator=(WAL&& other) noexcept;

    WAL(const WAL&) = delete;
    WAL& operator=(const WAL&) = delete;

    // Create a new WAL file, truncating if it exists.
    Status create(const std::string& path);

    // Open an existing WAL file.
    Status open(const std::string& path);

    // Close the WAL file. Discards any uncommitted data.
    Status close();

    // Check if the WAL file is open.
    bool isOpen() const;

    // Current file size (bytes written so far).
    uint64_t size() const;

    // Buffer a data record. No I/O.
    void put(const void* data, size_t len);

    // Write all buffered records + commit record to disk in one append.
    // Clears the buffer. Optionally syncs.
    Status commit(bool sync = false);

    // Discard buffered records without writing.
    void abort();

    // Replay callback receives a monotonic sequence number and a vector of
    // string_views into the payloads of data records in that transaction.
    using ReplayCallback = std::function<Status(
        uint64_t seq,
        const std::vector<std::string_view>& records)>;

    // Replay all committed transactions from the file.
    // valid_end is set to the file offset past the last valid commit record.
    Status replay(const ReplayCallback& cb, uint64_t& valid_end) const;

    // Replay all committed transactions and truncate the file at valid_end.
    Status replayAndTruncate(const ReplayCallback& cb);

    // Truncate the file to zero length and reset state.
    Status truncate();

    // Sync data to disk.
    Status sync();

private:
    static constexpr uint8_t kTypeData = 0x01;
    static constexpr uint8_t kTypeCommit = 0x02;

    LogFile log_file_;
    std::vector<uint8_t> batch_buf_;
    uint64_t next_seq_ = 0;
};

} // namespace internal
} // namespace kvlite

#endif // KVLITE_INTERNAL_WAL_H
