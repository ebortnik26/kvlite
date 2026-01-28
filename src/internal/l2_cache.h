#ifndef KVLITE_INTERNAL_L2_CACHE_H
#define KVLITE_INTERNAL_L2_CACHE_H

#include <cstdint>
#include <string>
#include <memory>
#include <list>
#include <unordered_map>
#include <shared_mutex>
#include <functional>

#include "kvlite/status.h"
#include "l2_index.h"

namespace kvlite {
namespace internal {

// LRU cache for L2 indices.
//
// L2 indices are loaded on-demand when a read needs to find the offset
// for a key within a specific log file. The cache evicts least-recently-used
// indices when the total memory usage exceeds the configured limit.
//
// Thread-safety: All operations are thread-safe.
class L2Cache {
public:
    // Create cache with maximum memory budget (in bytes)
    explicit L2Cache(const std::string& db_path, size_t max_memory = 256 * 1024 * 1024);
    ~L2Cache();

    // Get an L2 index, loading from disk if not cached.
    // Returns nullptr on error (e.g., file not found).
    // The returned pointer is valid until the next eviction.
    // Caller should use getAndUse() for safer access pattern.
    L2Index* get(uint32_t file_id);

    // Get an L2 index and execute a function with it.
    // This is the preferred access pattern as it handles locking properly.
    // Returns the result of the function, or error if index couldn't be loaded.
    template<typename Func>
    auto withIndex(uint32_t file_id, Func&& fn)
        -> decltype(fn(std::declval<L2Index&>()));

    // Insert a pre-built index (e.g., after creating a new log file)
    void put(uint32_t file_id, std::unique_ptr<L2Index> index);

    // Explicitly evict an index (e.g., after deleting log file during GC)
    void evict(uint32_t file_id);

    // Clear the entire cache
    void clear();

    // Get statistics
    size_t cachedCount() const;
    size_t memoryUsage() const;
    size_t maxMemory() const { return max_memory_; }
    uint64_t hits() const { return hits_; }
    uint64_t misses() const { return misses_; }

    // Set maximum memory (triggers eviction if over limit)
    void setMaxMemory(size_t max_memory);

private:
    Status loadIndex(uint32_t file_id, std::unique_ptr<L2Index>& index);
    void evictIfNeeded();
    void touchLRU(uint32_t file_id);

    std::string db_path_;
    size_t max_memory_;

    // LRU list: front = most recently used, back = least recently used
    std::list<uint32_t> lru_list_;

    // Map from file_id to (index, lru_iterator)
    struct CacheEntry {
        std::unique_ptr<L2Index> index;
        std::list<uint32_t>::iterator lru_iter;
    };
    std::unordered_map<uint32_t, CacheEntry> cache_;

    // Current memory usage
    size_t current_memory_ = 0;

    // Statistics
    mutable uint64_t hits_ = 0;
    mutable uint64_t misses_ = 0;

    mutable std::shared_mutex mutex_;
};

// Template implementation
template<typename Func>
auto L2Cache::withIndex(uint32_t file_id, Func&& fn)
    -> decltype(fn(std::declval<L2Index&>())) {
    std::shared_lock<std::shared_mutex> lock(mutex_);

    auto it = cache_.find(file_id);
    if (it != cache_.end()) {
        hits_++;
        // Move to upgrade lock for LRU update
        lock.unlock();
        {
            std::unique_lock<std::shared_mutex> write_lock(mutex_);
            touchLRU(file_id);
        }
        lock.lock();
        it = cache_.find(file_id);
        if (it != cache_.end()) {
            return fn(*it->second.index);
        }
    }

    // Need to load - upgrade to write lock
    lock.unlock();
    std::unique_lock<std::shared_mutex> write_lock(mutex_);

    // Double-check after acquiring write lock
    it = cache_.find(file_id);
    if (it != cache_.end()) {
        touchLRU(file_id);
        return fn(*it->second.index);
    }

    misses_++;
    std::unique_ptr<L2Index> index;
    Status s = loadIndex(file_id, index);
    if (!s.ok()) {
        throw std::runtime_error("Failed to load L2 index: " + s.message());
    }

    L2Index* ptr = index.get();
    current_memory_ += index->memoryUsage();

    CacheEntry entry;
    entry.index = std::move(index);
    lru_list_.push_front(file_id);
    entry.lru_iter = lru_list_.begin();
    cache_[file_id] = std::move(entry);

    evictIfNeeded();

    return fn(*ptr);
}

} // namespace internal
} // namespace kvlite

#endif // KVLITE_INTERNAL_L2_CACHE_H
