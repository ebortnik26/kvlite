#include <chrono>
#include <cstdio>
#include <string>
#include <vector>

#include "internal/bit_stream.h"
#include "internal/delta_hash_table.h"

using namespace kvlite::internal;

static double now_ms() {
    auto t = std::chrono::high_resolution_clock::now();
    return std::chrono::duration<double, std::milli>(t.time_since_epoch()).count();
}

static void bench_bitstream_read() {
    constexpr int N = 1'000'000;
    uint8_t buf[1024] = {};
    // Fill with some data
    for (int i = 0; i < 1024; ++i) buf[i] = i & 0xFF;

    double start = now_ms();
    uint64_t sink = 0;
    for (int iter = 0; iter < N; ++iter) {
        BitReader reader(buf, 0);
        for (int j = 0; j < 10; ++j) {
            sink += reader.read(39);
            sink += reader.readUnary();
            sink += reader.read(64);
        }
    }
    double elapsed = now_ms() - start;
    printf("BitStream read:  %8.2f ms  (%d iters, sink=%lu)\n", elapsed, N, sink);
}

static void bench_bitstream_write() {
    constexpr int N = 1'000'000;
    uint8_t buf[1024] = {};

    double start = now_ms();
    for (int iter = 0; iter < N; ++iter) {
        BitWriter writer(buf, 0);
        for (int j = 0; j < 10; ++j) {
            writer.write(0x1234567ULL, 39);
            writer.writeUnary(3);
            writer.write(0xDEADBEEFCAFEULL, 64);
        }
    }
    double elapsed = now_ms() - start;
    printf("BitStream write: %8.2f ms  (%d iters)\n", elapsed, N);
}

static DeltaHashTable::Config benchConfig() {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 14;     // 16K buckets
    cfg.lslot_bits = 4;       // 16 lslots
    cfg.bucket_bytes = 512;
    return cfg;
}

static void bench_dht_add(int N) {
    DeltaHashTable dht(benchConfig());

    std::vector<std::string> keys(N);
    for (int i = 0; i < N; ++i) {
        keys[i] = "key_" + std::to_string(i);
    }

    double start = now_ms();
    for (int i = 0; i < N; ++i) {
        dht.addEntry(keys[i], static_cast<uint32_t>(i * 10 + 1));
    }
    double elapsed = now_ms() - start;
    printf("DHT add    %6dk: %8.2f ms  (%5.0f ns/op, mem=%zu KB)\n",
           N/1000, elapsed, elapsed * 1e6 / N, dht.memoryUsage() / 1024);
}

static void bench_dht_find(int N) {
    DeltaHashTable dht(benchConfig());

    std::vector<std::string> keys(N);
    for (int i = 0; i < N; ++i) {
        keys[i] = "key_" + std::to_string(i);
        dht.addEntry(keys[i], static_cast<uint32_t>(i + 1));
    }

    double start = now_ms();
    int found = 0;
    for (int iter = 0; iter < 3; ++iter) {
        for (int i = 0; i < N; ++i) {
            uint32_t val;
            if (dht.findFirst(keys[i], val)) ++found;
        }
    }
    double elapsed = now_ms() - start;
    printf("DHT find   %6dk: %8.2f ms  (%5.0f ns/op, found=%d)\n",
           N/1000, elapsed, elapsed * 1e6 / (3 * N), found);
}

int main() {
    printf("=== BitStream ===\n");
    bench_bitstream_read();
    bench_bitstream_write();

    printf("\n=== DeltaHashTable ===\n");
    bench_dht_add(10'000);
    bench_dht_add(100'000);
    bench_dht_find(10'000);
    bench_dht_find(100'000);

    return 0;
}
