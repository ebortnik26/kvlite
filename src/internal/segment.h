#ifndef KVLITE_INTERNAL_SEGMENT_H
#define KVLITE_INTERNAL_SEGMENT_H

#include <cstdint>
#include <string>
#include <vector>

#include "internal/l2_index.h"
#include "internal/log_file.h"

namespace kvlite {
namespace internal {

// A Segment pairs one LogFile with one L2Index.
//
// All data and index operations go through this class;
// the underlying LogFile and L2Index are not exposed.
class Segment {
public:
    Segment();
    ~Segment();

    Segment(const Segment&) = delete;
    Segment& operator=(const Segment&) = delete;
    Segment(Segment&&) noexcept;
    Segment& operator=(Segment&&) noexcept;

    // --- Lifecycle ---

    Status create(const std::string& path);
    Status load(const std::string& data_path, const std::string& index_path);
    Status saveIndex(const std::string& path);
    Status close();
    bool isOpen() const;

    // --- Write ---

    Status append(const void* data, size_t len, uint64_t& offset);
    void addIndex(const std::string& key, uint32_t offset, uint32_t version);

    // --- Read ---

    Status readAt(uint64_t offset, void* buf, size_t len);

    // --- Index queries ---

    bool getLatest(const std::string& key,
                   uint32_t& offset, uint32_t& version) const;

    bool get(const std::string& key,
             std::vector<uint32_t>& offsets,
             std::vector<uint32_t>& versions) const;

    bool get(const std::string& key, uint64_t upper_bound,
             uint64_t& offset, uint64_t& version) const;

    bool contains(const std::string& key) const;

    // --- Stats ---

    uint64_t size() const;
    size_t keyCount() const;
    size_t entryCount() const;

private:
    LogFile log_file_;
    L2Index index_;
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_SEGMENT_H
