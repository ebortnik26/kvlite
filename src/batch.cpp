#include "kvlite/batch.h"

namespace kvlite {

// --- WriteBatch ---

void WriteBatch::put(const std::string& key, const std::string& value) {
    operations_.push_back({OpType::kPut, key, value});
    approximate_size_ += key.size() + value.size() + 16;
}

void WriteBatch::remove(const std::string& key) {
    operations_.push_back({OpType::kDelete, key, ""});
    approximate_size_ += key.size() + 16;
}

void WriteBatch::clear() {
    operations_.clear();
    approximate_size_ = 0;
}

// --- ReadBatch ---

void ReadBatch::get(const std::string& key) {
    keys_.push_back(key);
}

void ReadBatch::get(const std::vector<std::string>& keys) {
    keys_.insert(keys_.end(), keys.begin(), keys.end());
}

void ReadBatch::clear() {
    keys_.clear();
    results_.clear();
    snapshot_version_ = 0;
}

}  // namespace kvlite
