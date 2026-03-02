// Basic kvlite usage: open, put, get, remove, close.

#include <kvlite/kvlite.h>
#include <iostream>

int main() {
    kvlite::DB db;
    kvlite::Options options;
    options.create_if_missing = true;

    kvlite::Status s = db.open("/tmp/kvlite_basic_example", options);
    if (!s.ok()) {
        std::cerr << "Failed to open DB: " << s.message() << std::endl;
        return 1;
    }

    // Put a key-value pair (allocates a new version).
    db.put("greeting", "hello");

    // Read it back.
    std::string value;
    uint64_t version;
    s = db.get("greeting", value, version);
    if (s.ok()) {
        std::cout << "greeting = " << value << " (v" << version << ")" << std::endl;
    }

    // Overwrite the key (allocates another version).
    db.put("greeting", "world");
    db.get("greeting", value, version);
    std::cout << "greeting = " << value << " (v" << version << ")" << std::endl;

    // Check existence.
    bool found = false;
    db.exists("greeting", found);
    std::cout << "exists: " << (found ? "yes" : "no") << std::endl;

    // Remove the key (creates a tombstone at a new version).
    db.remove("greeting");

    s = db.get("greeting", value);
    std::cout << "after remove: " << s.toString() << std::endl;

    // Print stats.
    kvlite::DBStats stats;
    db.getStats(stats);
    std::cout << "current version: " << stats.current_version << std::endl;
    std::cout << "live entries:    " << stats.num_live_entries << std::endl;

    db.close();
    return 0;
}
