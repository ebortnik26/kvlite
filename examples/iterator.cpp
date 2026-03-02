// Iterator usage: unordered scan over all live keys.

#include <kvlite/kvlite.h>
#include <iostream>
#include <map>

int main() {
    kvlite::DB db;
    kvlite::Options options;
    options.create_if_missing = true;

    kvlite::Status s = db.open("/tmp/kvlite_iterator_example", options);
    if (!s.ok()) {
        std::cerr << "Failed to open DB: " << s.message() << std::endl;
        return 1;
    }

    // Populate some data.
    db.put("fruit:apple",  "red");
    db.put("fruit:banana", "yellow");
    db.put("fruit:grape",  "purple");
    db.put("fruit:lime",   "green");

    // Overwrite one key to create a second version.
    db.put("fruit:apple", "green");

    // Remove one key (creates a tombstone).
    db.remove("fruit:lime");

    // --- Full scan using an iterator ---
    // The iterator captures a snapshot at creation time.
    // It sees only the latest version of each key at that snapshot,
    // and skips tombstoned keys.

    std::unique_ptr<kvlite::Iterator> iter;
    s = db.createIterator(iter);
    if (!s.ok()) {
        std::cerr << "Failed to create iterator: " << s.message() << std::endl;
        return 1;
    }

    std::cout << "Iterator snapshot at version "
              << iter->snapshot().version() << std::endl;

    // Keys arrive in arbitrary (hash) order.
    // Collect into a sorted map for display.
    std::map<std::string, std::string> entries;
    std::string key, value;
    uint64_t version;
    size_t count = 0;
    while (iter->next(key, value, version).ok()) {
        std::cout << "  scanned: " << key << " = " << value
                  << " (v" << version << ")" << std::endl;
        entries[key] = value;
        ++count;
    }

    std::cout << "\nTotal keys scanned: " << count << std::endl;
    std::cout << "Sorted:\n";
    for (const auto& [k, v] : entries) {
        std::cout << "  " << k << " = " << v << std::endl;
    }

    // Release iterator before closing the DB.
    iter.reset();
    db.close();
    return 0;
}
