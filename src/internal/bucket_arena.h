#ifndef KVLITE_INTERNAL_BUCKET_ARENA_H
#define KVLITE_INTERNAL_BUCKET_ARENA_H

#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

namespace kvlite {
namespace internal {

struct Bucket {
    uint8_t* data = nullptr;
};

// Chunk-based bump allocator for fixed-size slots.
//
// Each slot is slot_size bytes: a Bucket header followed by bucket data.
// allocate() returns a 1-based index; get() converts that to a stable pointer.
// Slots are allocated in chunks of kSlotsPerChunk to avoid per-slot heap allocs.
//
// If concurrent=true, allocate() is internally synchronized with a mutex.
// get() is always lock-free (stable pointers, chunk vector pre-reserved).
// clear() is a single-threaded lifecycle event (not synchronized).
class BucketArena {
public:
    static constexpr uint32_t kSlotsPerChunk = 64;

    explicit BucketArena(uint32_t slot_size, bool concurrent = false);

    BucketArena(const BucketArena&) = delete;
    BucketArena& operator=(const BucketArena&) = delete;
    BucketArena(BucketArena&& other) noexcept;
    BucketArena& operator=(BucketArena&& other) noexcept;

    uint32_t allocate();                        // 1-based index; locks if concurrent
    Bucket* get(uint32_t one_based);            // stable pointer, lock-free
    const Bucket* get(uint32_t one_based) const;
    uint32_t size() const;
    size_t dataBytes() const;                   // size_ * (slot_size_ - sizeof(Bucket))
    void clear();                               // single-threaded lifecycle event

private:
    uint32_t allocateImpl();    // unsynchronized core
    uint32_t slot_size_;
    bool concurrent_;
    uint32_t size_ = 0;
    std::vector<std::unique_ptr<uint8_t[]>> chunks_;
    std::mutex mutex_;          // only engaged when concurrent_
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_BUCKET_ARENA_H
