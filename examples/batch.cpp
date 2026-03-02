// Batch operations: atomic multi-key writes and consistent multi-key reads.

#include <kvlite/kvlite.h>
#include <iostream>

int main() {
    kvlite::DB db;
    kvlite::Options options;
    options.create_if_missing = true;

    kvlite::Status s = db.open("/tmp/kvlite_batch_example", options);
    if (!s.ok()) {
        std::cerr << "Failed to open DB: " << s.message() << std::endl;
        return 1;
    }

    // --- WriteBatch: all operations get the same version ---

    kvlite::WriteBatch wb;
    wb.put("user:1:name",  "Alice");
    wb.put("user:1:email", "alice@example.com");
    wb.put("user:1:role",  "admin");

    s = db.write(wb);
    if (!s.ok()) {
        std::cerr << "WriteBatch failed: " << s.message() << std::endl;
        return 1;
    }

    std::cout << "WriteBatch committed (" << wb.operations().size()
              << " operations)" << std::endl;

    // --- ReadBatch: all reads at the same consistent snapshot ---

    kvlite::ReadBatch rb;
    rb.get("user:1:name");
    rb.get("user:1:email");
    rb.get("user:1:role");
    rb.get("user:1:missing");  // This key does not exist.

    db.read(rb);

    std::cout << "\nReadBatch at version " << rb.snapshotVersion() << ":\n";
    for (const auto& result : rb.results()) {
        if (result.status.ok()) {
            std::cout << "  " << result.key << " = " << result.value
                      << " (v" << result.version << ")" << std::endl;
        } else if (result.status.isNotFound()) {
            std::cout << "  " << result.key << " [not found]" << std::endl;
        }
    }

    // --- Overwrite with a second WriteBatch ---

    kvlite::WriteBatch wb2;
    wb2.put("user:1:name",  "Alice Smith");
    wb2.put("user:1:email", "alice.smith@example.com");
    db.write(wb2);

    // The two keys now have a newer version than "role".
    std::string value;
    uint64_t version;
    db.get("user:1:name", value, version);
    std::cout << "\nAfter update: user:1:name = " << value
              << " (v" << version << ")" << std::endl;

    db.get("user:1:role", value, version);
    std::cout << "Unchanged:    user:1:role = " << value
              << " (v" << version << ")" << std::endl;

    db.close();
    return 0;
}
