#ifndef KVLITE_ITERATOR_H
#define KVLITE_ITERATOR_H

#include <cstdint>
#include <memory>
#include <string>

#include "kvlite/status.h"

namespace kvlite {

class DB;
class Snapshot;

class Iterator {
public:
    ~Iterator();

    Iterator(const Iterator&) = delete;
    Iterator& operator=(const Iterator&) = delete;

    Iterator(Iterator&& other) noexcept;
    Iterator& operator=(Iterator&& other) noexcept;

    Status next(std::string& key, std::string& value);
    Status next(std::string& key, std::string& value, uint64_t& version);

    const Snapshot& snapshot() const;

private:
    friend class DB;
    class Impl;
    explicit Iterator(std::unique_ptr<Impl> impl);
    std::unique_ptr<Impl> impl_;
};

}  // namespace kvlite

#endif  // KVLITE_ITERATOR_H
