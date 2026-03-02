// Snapshot usage: point-in-time reads that are isolated from concurrent writes.

#include <kvlite/kvlite.h>
#include <iostream>

int main() {
    kvlite::DB db;
    kvlite::Options options;
    options.create_if_missing = true;

    kvlite::Status s = db.open("/tmp/kvlite_snapshot_example", options);
    if (!s.ok()) {
        std::cerr << "Failed to open DB: " << s.message() << std::endl;
        return 1;
    }

    // Write initial data.
    db.put("counter", "100");
    db.put("name", "alice");

    // Take snapshot A — captures the current state.
    kvlite::Snapshot snapA = db.createSnapshot();
    std::cout << "Snapshot A at version " << snapA.version() << std::endl;

    // Mutate the database after the snapshot.
    db.put("counter", "200");
    db.put("name", "bob");

    // Take snapshot B — captures the updated state.
    kvlite::Snapshot snapB = db.createSnapshot();

    // Mutate again.
    db.put("counter", "300");

    // Read through snapshot A — sees the original values.
    kvlite::ReadOptions roA;
    roA.snapshot = &snapA;

    std::string value;
    db.get("counter", value, roA);
    std::cout << "Snapshot A counter: " << value << std::endl;  // "100"
    db.get("name", value, roA);
    std::cout << "Snapshot A name:    " << value << std::endl;  // "alice"

    // Read through snapshot B — sees the first update.
    kvlite::ReadOptions roB;
    roB.snapshot = &snapB;

    db.get("counter", value, roB);
    std::cout << "Snapshot B counter: " << value << std::endl;  // "200"
    db.get("name", value, roB);
    std::cout << "Snapshot B name:    " << value << std::endl;  // "bob"

    // Read latest — sees the most recent write.
    db.get("counter", value);
    std::cout << "Latest counter:     " << value << std::endl;  // "300"

    // Release snapshots so GC can reclaim old versions.
    db.releaseSnapshot(snapA);
    db.releaseSnapshot(snapB);

    // Iterator also uses a snapshot internally.
    {
        std::unique_ptr<kvlite::Iterator> iter;
        db.createIterator(iter);
        std::cout << "\nIterator at version " << iter->snapshot().version() << std::endl;

        std::string key;
        uint64_t version;
        while (iter->next(key, value, version).ok()) {
            std::cout << "  " << key << " = " << value
                      << " (v" << version << ")" << std::endl;
        }
    }  // Iterator released before close.

    db.close();
    return 0;
}
