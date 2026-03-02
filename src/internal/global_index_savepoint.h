#ifndef KVLITE_INTERNAL_GLOBAL_INDEX_SAVEPOINT_H
#define KVLITE_INTERNAL_GLOBAL_INDEX_SAVEPOINT_H

#include <atomic>
#include <cstdint>
#include <string>
#include <vector>

#include "kvlite/status.h"

namespace kvlite {
namespace internal {

class ReadWriteDeltaHashTable;

namespace savepoint {

struct FileDesc {
    uint32_t file_index;
    uint32_t bucket_start;
    uint32_t bucket_count;
    bool is_last;
};

std::vector<FileDesc> computeFileLayout(
    const ReadWriteDeltaHashTable& dht, uint8_t num_threads);

Status storeFile(const std::string& dir, const FileDesc& fd,
                 const ReadWriteDeltaHashTable& dht, uint64_t key_count);

Status store(const std::string& dir, const ReadWriteDeltaHashTable& dht,
             uint64_t key_count, uint8_t num_threads);

Status loadFile(const std::string& fpath, uint32_t stride,
                ReadWriteDeltaHashTable& dht,
                uint64_t& out_entries, uint64_t& out_key_count,
                uint32_t& out_ext_count);

Status load(const std::string& dir, ReadWriteDeltaHashTable& dht,
            std::atomic<size_t>& key_count, uint8_t num_threads);

}  // namespace savepoint
}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_GLOBAL_INDEX_SAVEPOINT_H
