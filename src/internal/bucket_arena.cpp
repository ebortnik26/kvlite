#include "internal/bucket_arena.h"

#include <cstring>

namespace kvlite {
namespace internal {

BucketArena::BucketArena(uint32_t slot_size, bool concurrent)
    : slot_size_(slot_size), concurrent_(concurrent) {
    chunks_.reserve(256);
}

BucketArena::BucketArena(BucketArena&& other) noexcept
    : slot_size_(other.slot_size_),
      concurrent_(other.concurrent_),
      size_(other.size_),
      chunks_(std::move(other.chunks_)) {
    other.size_ = 0;
}

BucketArena& BucketArena::operator=(BucketArena&& other) noexcept {
    if (this != &other) {
        slot_size_ = other.slot_size_;
        concurrent_ = other.concurrent_;
        size_ = other.size_;
        chunks_ = std::move(other.chunks_);
        other.size_ = 0;
    }
    return *this;
}

uint32_t BucketArena::allocate() {
    if (concurrent_) {
        std::lock_guard<std::mutex> g(mutex_);
        return allocateImpl();
    }
    return allocateImpl();
}

uint32_t BucketArena::allocateImpl() {
    uint32_t slot_in_chunk = size_ % kSlotsPerChunk;
    if (slot_in_chunk == 0) {
        size_t chunk_bytes = static_cast<size_t>(kSlotsPerChunk) * slot_size_;
        auto chunk = std::make_unique<uint8_t[]>(chunk_bytes);
        std::memset(chunk.get(), 0, chunk_bytes);
        chunks_.push_back(std::move(chunk));
    }
    ++size_;
    // Wire up Bucket::data to point at the data portion of this slot.
    Bucket* b = get(size_);  // 1-based
    uint8_t* slot_base = reinterpret_cast<uint8_t*>(b);
    b->data = slot_base + sizeof(Bucket);
    return size_;  // 1-based index
}

Bucket* BucketArena::get(uint32_t one_based) {
    uint32_t idx = one_based - 1;
    uint32_t chunk = idx / kSlotsPerChunk;
    uint32_t offset = idx % kSlotsPerChunk;
    return reinterpret_cast<Bucket*>(
        chunks_[chunk].get() + static_cast<size_t>(offset) * slot_size_);
}

const Bucket* BucketArena::get(uint32_t one_based) const {
    uint32_t idx = one_based - 1;
    uint32_t chunk = idx / kSlotsPerChunk;
    uint32_t offset = idx % kSlotsPerChunk;
    return reinterpret_cast<const Bucket*>(
        chunks_[chunk].get() + static_cast<size_t>(offset) * slot_size_);
}

uint32_t BucketArena::size() const { return size_; }

size_t BucketArena::dataBytes() const {
    return static_cast<size_t>(size_) * (slot_size_ - sizeof(Bucket));
}

void BucketArena::clear() {
    chunks_.clear();
    size_ = 0;
}

}  // namespace internal
}  // namespace kvlite
