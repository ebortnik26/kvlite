#ifndef KVLITE_DB_H
#define KVLITE_DB_H

#include <memory>
#include <string>

#include "kvlite/status.h"
#include "kvlite/options.h"
#include "kvlite/write_batch.h"

namespace kvlite {

class DBImpl;

class DB {
public:
    DB();
    ~DB();

    // Non-copyable
    DB(const DB&) = delete;
    DB& operator=(const DB&) = delete;

    // Movable
    DB(DB&& other) noexcept;
    DB& operator=(DB&& other) noexcept;

    // Open a database at the specified path
    Status open(const std::string& path, const Options& options = Options());

    // Close the database
    void close();

    // Check if the database is open
    bool isOpen() const;

    // Set the value for the given key
    Status put(const std::string& key, const std::string& value,
               const WriteOptions& options = WriteOptions());

    // Get the value for the given key
    Status get(const std::string& key, std::string* value,
               const ReadOptions& options = ReadOptions());

    // Remove the entry for the given key
    Status remove(const std::string& key,
                  const WriteOptions& options = WriteOptions());

    // Check if a key exists
    bool exists(const std::string& key,
                const ReadOptions& options = ReadOptions());

    // Apply a batch of writes atomically
    Status write(const WriteBatch& batch,
                 const WriteOptions& options = WriteOptions());

    // Force a compaction of the underlying storage
    Status compact();

    // Get database statistics
    std::string getStats() const;

private:
    std::unique_ptr<DBImpl> impl_;
};

} // namespace kvlite

#endif // KVLITE_DB_H
