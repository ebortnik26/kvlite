#ifndef KVLITE_INTERNAL_DUPLICATES_BUCKET_BITMAP_H
#define KVLITE_INTERNAL_DUPLICATES_BUCKET_BITMAP_H

#include <atomic>
#include <cstdint>
#include <memory>

namespace kvlite {
namespace internal {

// Lock-free bitmap (1 bit per primary bucket) that tracks which buckets
// currently hold multi-version keys.  Used by the background dedup
// daemon to skip clean buckets cheaply.
//
// Thread-safety: single-byte atomic fetch_or / fetch_and are race-free
// without any locking.  Concurrent set/clear races are benign (at
// worst the daemon revisits a bucket on the next tick).
class DuplicatesBucketBitmap {
public:
    explicit DuplicatesBucketBitmap(uint32_t num_buckets)
        : num_buckets_(num_buckets),
          bytes_(std::make_unique<std::atomic<uint8_t>[]>((num_buckets + 7) / 8)) {
        uint32_t n = (num_buckets + 7) / 8;
        for (uint32_t i = 0; i < n; ++i) {
            bytes_[i].store(0, std::memory_order_relaxed);
        }
    }

    void set(uint32_t bi) {
        bytes_[bi >> 3].fetch_or(
            static_cast<uint8_t>(1u << (bi & 7)),
            std::memory_order_relaxed);
    }

    void clear(uint32_t bi) {
        bytes_[bi >> 3].fetch_and(
            static_cast<uint8_t>(~(1u << (bi & 7))),
            std::memory_order_relaxed);
    }

    bool test(uint32_t bi) const {
        return bytes_[bi >> 3].load(std::memory_order_relaxed)
               & static_cast<uint8_t>(1u << (bi & 7));
    }

    void clearAll() {
        uint32_t n = (num_buckets_ + 7) / 8;
        for (uint32_t i = 0; i < n; ++i) {
            bytes_[i].store(0, std::memory_order_relaxed);
        }
    }

    uint32_t numBuckets() const { return num_buckets_; }

private:
    uint32_t num_buckets_;
    std::unique_ptr<std::atomic<uint8_t>[]> bytes_;
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_DUPLICATES_BUCKET_BITMAP_H
