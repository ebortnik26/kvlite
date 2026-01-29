#include "internal/l2_index.h"

namespace kvlite {
namespace internal {

static L2DeltaHashTable::Config defaultDHTConfig() {
    L2DeltaHashTable::Config config;
    config.bucket_bits = 16;
    config.lslot_bits = 4;
    config.bucket_bytes = 256;
    return config;
}

L2Index::L2Index() : dht_(defaultDHTConfig()) {}

L2Index::~L2Index() = default;

void L2Index::put(const std::string& key, uint32_t offset, uint32_t version) {
    bool is_new = !dht_.contains(key);
    dht_.addEntry(key, offset, version);
    if (is_new) {
        ++key_count_;
    }
}

bool L2Index::get(const std::string& key,
                  std::vector<uint32_t>& offsets,
                  std::vector<uint32_t>& versions) const {
    return dht_.findAll(key, offsets, versions);
}

bool L2Index::getLatest(const std::string& key,
                        uint32_t& offset, uint32_t& version) const {
    return dht_.findFirst(key, offset, version);
}

bool L2Index::contains(const std::string& key) const {
    return dht_.contains(key);
}

void L2Index::forEach(
    const std::function<void(const std::vector<uint32_t>&,
                             const std::vector<uint32_t>&)>& fn) const {
    dht_.forEachGroup([&fn](uint64_t /*hash*/,
                            const std::vector<uint32_t>& offsets,
                            const std::vector<uint32_t>& versions) {
        fn(offsets, versions);
    });
}

size_t L2Index::keyCount() const {
    return key_count_;
}

size_t L2Index::entryCount() const {
    return dht_.size();
}

size_t L2Index::memoryUsage() const {
    return dht_.memoryUsage();
}

void L2Index::clear() {
    dht_.clear();
    key_count_ = 0;
}

}  // namespace internal
}  // namespace kvlite
