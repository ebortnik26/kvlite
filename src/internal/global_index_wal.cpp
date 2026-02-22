#include "internal/global_index_wal.h"

namespace kvlite {
namespace internal {

GlobalIndexWAL::GlobalIndexWAL() = default;

GlobalIndexWAL::~GlobalIndexWAL() {
    if (fd_ >= 0) {
        close();
    }
}

Status GlobalIndexWAL::open(const std::string& path) {
    path_ = path;
    return Status::OK();
}

Status GlobalIndexWAL::close() {
    fd_ = -1;
    return Status::OK();
}

Status GlobalIndexWAL::appendPut(const std::string& /*key*/, uint64_t /*version*/,
                                  uint32_t /*segment_id*/) {
    return Status::OK();
}

Status GlobalIndexWAL::appendRemove(const std::string& /*key*/) {
    return Status::OK();
}

Status GlobalIndexWAL::sync() {
    return Status::OK();
}

Status GlobalIndexWAL::replay(GlobalIndex& /*index*/) {
    return Status::OK();
}

Status GlobalIndexWAL::truncate() {
    entry_count_ = 0;
    return Status::OK();
}

uint64_t GlobalIndexWAL::size() const {
    return 0;
}

Status GlobalIndexWAL::writeEntry(WalOp /*op*/, const std::string& /*key*/,
                                   uint64_t /*version*/, uint32_t /*segment_id*/) {
    return Status::OK();
}

Status GlobalIndexWAL::readEntry(WalOp& /*op*/, std::string& /*key*/,
                                  uint64_t& /*version*/, uint32_t& /*segment_id*/) {
    return Status::OK();
}

std::string GlobalIndexWAL::makePath(const std::string& db_path) {
    return db_path + "/global_index.wal";
}

}  // namespace internal
}  // namespace kvlite
