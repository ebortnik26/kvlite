#ifndef KVLITE_INTERNAL_BUCKET_STATS_H
#define KVLITE_INTERNAL_BUCKET_STATS_H

#include <cstdint>
#include <cstring>
#include <memory>

namespace kvlite {
namespace internal {

// Per-bucket packed bitmap (1 bit per bucket).
class BucketBitmap {
public:
    explicit BucketBitmap(uint32_t num_buckets)
        : size_(num_buckets),
          data_(std::make_unique<uint8_t[]>((num_buckets + 7) / 8)) {
        std::memset(data_.get(), 0, (num_buckets + 7) / 8);
    }

    void set(uint32_t i)   { data_[i / 8] |=  (1u << (i % 8)); }
    void clear(uint32_t i) { data_[i / 8] &= ~(1u << (i % 8)); }
    bool test(uint32_t i) const { return data_[i / 8] & (1u << (i % 8)); }
    void clearAll() { std::memset(data_.get(), 0, (size_ + 7) / 8); }

private:
    uint32_t size_;
    std::unique_ptr<uint8_t[]> data_;
};

// Per-bucket value array (one T per bucket).
template<typename T>
class BucketArray {
public:
    explicit BucketArray(uint32_t num_buckets)
        : size_(num_buckets),
          data_(std::make_unique<T[]>(num_buckets)) {
        std::memset(data_.get(), 0, num_buckets * sizeof(T));
    }

    T get(uint32_t i) const { return data_[i]; }
    void set(uint32_t i, T val) { data_[i] = val; }
    void add(uint32_t i, T delta) { data_[i] += delta; }

    // Sum all entries.
    uint64_t sum() const {
        uint64_t total = 0;
        for (uint32_t i = 0; i < size_; ++i) total += data_[i];
        return total;
    }

private:
    uint32_t size_;
    std::unique_ptr<T[]> data_;
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_BUCKET_STATS_H
