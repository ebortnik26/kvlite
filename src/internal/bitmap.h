#ifndef KVLITE_INTERNAL_BITMAP_H
#define KVLITE_INTERNAL_BITMAP_H

#include <cstdint>
#include <cstring>
#include <memory>

namespace kvlite {
namespace internal {

// Packed bit array. Not thread-safe — caller must synchronize.
class Bitmap {
public:
    explicit Bitmap(uint32_t num_bits)
        : size_(num_bits),
          data_(std::make_unique<uint8_t[]>((num_bits + 7) / 8)) {
        std::memset(data_.get(), 0, (num_bits + 7) / 8);
    }

    void set(uint32_t i)   { data_[i / 8] |=  (1u << (i % 8)); }
    void clear(uint32_t i) { data_[i / 8] &= ~(1u << (i % 8)); }
    bool test(uint32_t i) const { return data_[i / 8] & (1u << (i % 8)); }

    void clearAll() { std::memset(data_.get(), 0, (size_ + 7) / 8); }
    uint32_t size() const { return size_; }

private:
    uint32_t size_;
    std::unique_ptr<uint8_t[]> data_;
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_BITMAP_H
