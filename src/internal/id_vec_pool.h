#ifndef KVLITE_INTERNAL_ID_VEC_POOL_H
#define KVLITE_INTERNAL_ID_VEC_POOL_H

#include <cstdint>
#include <mutex>
#include <vector>

namespace kvlite {
namespace internal {

// Pool allocator for std::vector<uint32_t>.
//
// Manages a set of id-vectors that can be allocated and released on demand.
// Released vectors are recycled via a free-list, avoiding repeated heap
// allocations for workloads that frequently promote/demote between inline
// and overflow representations.
//
// Handles are 32-bit indices into an internal flat array.
//
// Thread-safety: allocate() and release() are mutex-protected.
// Access to the vectors themselves (via get()) must be externally
// synchronized — in practice, the caller's bucket lock provides this.
class IdVecPool {
public:
    // Allocate a new (empty) vector. Returns its handle.
    uint32_t allocate() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!free_list_.empty()) {
            uint32_t handle = free_list_.back();
            free_list_.pop_back();
            return handle;
        }
        uint32_t handle = static_cast<uint32_t>(vecs_.size());
        vecs_.emplace_back();
        return handle;
    }

    // Release a vector back to the pool. Clears its contents and frees
    // the underlying storage.
    void release(uint32_t handle) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto& v = vecs_[handle];
        v.clear();
        v.shrink_to_fit();
        free_list_.push_back(handle);
    }

    // Access the vector by handle. Not thread-safe — caller must hold
    // an appropriate lock (e.g. DHT bucket lock).
    std::vector<uint32_t>& get(uint32_t handle) { return vecs_[handle]; }
    const std::vector<uint32_t>& get(uint32_t handle) const { return vecs_[handle]; }

    // Number of currently allocated (non-free) vectors.
    size_t activeCount() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return vecs_.size() - free_list_.size();
    }

    // Approximate memory usage in bytes.
    size_t memoryUsage() const {
        std::lock_guard<std::mutex> lock(mutex_);
        size_t usage = vecs_.capacity() * sizeof(std::vector<uint32_t>);
        for (const auto& v : vecs_) {
            usage += v.capacity() * sizeof(uint32_t);
        }
        usage += free_list_.capacity() * sizeof(uint32_t);
        return usage;
    }

    // Clear all vectors and the free-list.
    void clear() {
        std::lock_guard<std::mutex> lock(mutex_);
        vecs_.clear();
        free_list_.clear();
    }

private:
    std::vector<std::vector<uint32_t>> vecs_;
    std::vector<uint32_t> free_list_;
    mutable std::mutex mutex_;
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_ID_VEC_POOL_H
