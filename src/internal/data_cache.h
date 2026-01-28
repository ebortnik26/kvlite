#ifndef KVLITE_INTERNAL_DATA_CACHE_H
#define KVLITE_INTERNAL_DATA_CACHE_H

#include <cstdint>
#include <string>
#include <list>
#include <unordered_map>
#include <shared_mutex>
#include <functional>

namespace kvlite {
namespace internal {

// Read-only cache for key-value data.
//
// Caches recently read values to avoid repeated disk access for hot keys.
// Maps (key, version) → value.
//
// Thread-safety: All operations are thread-safe.
class DataCache {
public:
    explicit DataCache(size_t max_memory = 128 * 1024 * 1024);
    ~DataCache();

    // Try to get a value from cache.
    // Returns true if found, false if not cached.
    bool get(const std::string& key, uint64_t version, std::string& value);

    // Insert a value into the cache.
    // May evict older entries if memory limit exceeded.
    void put(const std::string& key, uint64_t version, const std::string& value);

    // Invalidate a specific (key, version) entry.
    void invalidate(const std::string& key, uint64_t version);

    // Invalidate all versions of a key.
    void invalidateKey(const std::string& key);

    // Clear the entire cache.
    void clear();

    // Statistics
    size_t entryCount() const;
    size_t memoryUsage() const;
    size_t maxMemory() const { return max_memory_; }
    uint64_t hits() const { return hits_; }
    uint64_t misses() const { return misses_; }

    // Adjust memory limit (triggers eviction if over)
    void setMaxMemory(size_t max_memory);

private:
    // Composite key: (key, version)
    struct CacheKey {
        std::string key;
        uint64_t version;

        bool operator==(const CacheKey& other) const {
            return version == other.version && key == other.key;
        }
    };

    struct CacheKeyHash {
        size_t operator()(const CacheKey& k) const {
            // Combine hash of key and version
            size_t h1 = std::hash<std::string>{}(k.key);
            size_t h2 = std::hash<uint64_t>{}(k.version);
            return h1 ^ (h2 + 0x9e3779b9 + (h1 << 6) + (h1 >> 2));
        }
    };

    struct CacheEntry {
        std::string value;
        std::list<CacheKey>::iterator lru_iter;
        size_t memory_size;  // key.size() + value.size() + overhead
    };

    static size_t entryMemorySize(const std::string& key, const std::string& value) {
        // Key stored twice (in map key and LRU list) + value + overhead
        return key.size() * 2 + sizeof(uint64_t) * 2 + value.size() + 64;
    }

    void evictIfNeeded();
    void touchLRU(const CacheKey& key);

    size_t max_memory_;
    size_t current_memory_ = 0;

    // LRU list: front = most recently used
    std::list<CacheKey> lru_list_;

    // Map from (key, version) to entry
    std::unordered_map<CacheKey, CacheEntry, CacheKeyHash> cache_;

    // Statistics
    mutable uint64_t hits_ = 0;
    mutable uint64_t misses_ = 0;

    mutable std::shared_mutex mutex_;
};

} // namespace internal
} // namespace kvlite

#endif // KVLITE_INTERNAL_DATA_CACHE_H
