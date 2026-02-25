#include <chrono>
#include <cstdio>
#include <string>
#include <vector>

#include "internal/bit_stream.h"

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

int main() {
    printf("=== BitStream ===\n");
    bench_bitstream_read();
    bench_bitstream_write();

    return 0;
}
