# kvlite

A high-performance persistent key-value store optimized for SSD drives.

## Features

- **SSD-Optimized**: Designed specifically for solid-state drive characteristics
- **Persistent Storage**: Data survives process restarts and system crashes
- **Simple API**: Easy-to-use C++ interface for key-value operations
- **High Performance**: Lock-free reads and efficient write batching
- **Lightweight**: Minimal dependencies, small memory footprint

## Requirements

- C++17 or later
- CMake 3.14+
- POSIX-compliant system (Linux, macOS)

## Building

```bash
mkdir build && cd build
cmake ..
make
```

To build with tests:
```bash
cmake -DKVLITE_BUILD_TESTS=ON ..
make
make test
```

## Installation

```bash
sudo make install
```

Or use as a CMake subdirectory:
```cmake
add_subdirectory(kvlite)
target_link_libraries(your_target kvlite)
```

## Quick Start

```cpp
#include <kvlite/kvlite.h>

int main() {
    // Open or create a database
    kvlite::DB db;
    kvlite::Options options;
    options.create_if_missing = true;

    auto status = db.open("/path/to/db", options);
    if (!status.ok()) {
        return 1;
    }

    // Write a key-value pair
    db.put("hello", "world");

    // Read a value
    std::string value;
    status = db.get("hello", &value);
    if (status.ok()) {
        std::cout << "Value: " << value << std::endl;
    }

    // Delete a key
    db.remove("hello");

    return 0;
}
```

## API Reference

### Opening a Database

```cpp
kvlite::DB db;
kvlite::Options options;
options.create_if_missing = true;    // Create DB if it doesn't exist
options.error_if_exists = false;     // Don't error if DB exists
options.write_buffer_size = 4 * 1024 * 1024;  // 4MB write buffer

auto status = db.open("/path/to/db", options);
```

### Basic Operations

```cpp
// Put a key-value pair
Status put(const std::string& key, const std::string& value);

// Get a value by key
Status get(const std::string& key, std::string* value);

// Delete a key
Status remove(const std::string& key);

// Check if a key exists
bool exists(const std::string& key);
```

### Batch Writes

```cpp
kvlite::WriteBatch batch;
batch.put("key1", "value1");
batch.put("key2", "value2");
batch.remove("key3");

db.write(batch);
```

## Architecture

```
┌─────────────────────────────────────────┐
│              Application                │
├─────────────────────────────────────────┤
│               kvlite API                │
├─────────────────────────────────────────┤
│  MemTable  │  Write-Ahead Log (WAL)     │
├────────────┴────────────────────────────┤
│           SSTable Manager               │
├─────────────────────────────────────────┤
│         Block Cache / Bloom Filter      │
├─────────────────────────────────────────┤
│              File System                │
└─────────────────────────────────────────┘
```

## Performance

Benchmarks on NVMe SSD (Samsung 980 Pro):

| Operation       | Throughput      |
|-----------------|-----------------|
| Random Write    | ~200,000 ops/s  |
| Random Read     | ~500,000 ops/s  |
| Sequential Read | ~1,000,000 ops/s|

## License

MIT License - see [LICENSE](LICENSE) for details.

## Contributing

Contributions are welcome! Please read [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Acknowledgments

Inspired by LevelDB, RocksDB, and other LSM-tree based storage engines.
