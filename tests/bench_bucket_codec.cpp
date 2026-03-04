#include <gtest/gtest.h>

#include <chrono>
#include <cstdio>
#include <cstring>
#include <random>
#include <vector>

#include "internal/bit_stream.h"
#include "internal/bucket_arena.h"
#include "internal/bucket_codec.h"

using namespace kvlite::internal;

// ============================================================
// Test data generators
// ============================================================

static BucketContents makeSequential(int num_keys, int versions_per_key) {
    BucketContents contents;
    for (int k = 0; k < num_keys; ++k) {
        KeyEntry entry;
        entry.suffix = static_cast<uint64_t>(k * 100 + 1);
        for (int v = 0; v < versions_per_key; ++v) {
            entry.packed_versions.push_back(
                static_cast<uint64_t>(1000 + k * 10 - v));
            entry.ids.push_back(static_cast<uint32_t>(k + 1));
        }
        contents.keys.push_back(std::move(entry));
    }
    return contents;
}

static BucketContents makeClustered(int num_keys, int versions_per_key) {
    BucketContents contents;
    uint64_t base_version = 100000;
    uint32_t base_id = 50;
    for (int k = 0; k < num_keys; ++k) {
        KeyEntry entry;
        entry.suffix = static_cast<uint64_t>(k * 37 + 1);
        for (int v = 0; v < versions_per_key; ++v) {
            entry.packed_versions.push_back(base_version + k * 3 - v);
            entry.ids.push_back(base_id + k / 4);
        }
        contents.keys.push_back(std::move(entry));
    }
    return contents;
}

static BucketContents makeRandom(int num_keys, int versions_per_key,
                                  uint32_t seed) {
    std::mt19937 rng(seed);
    BucketContents contents;

    std::vector<uint64_t> suffixes;
    for (int k = 0; k < num_keys; ++k) {
        suffixes.push_back(rng() & 0x0FFFFFFFULL);
    }
    std::sort(suffixes.begin(), suffixes.end());
    suffixes.erase(std::unique(suffixes.begin(), suffixes.end()), suffixes.end());

    for (size_t k = 0; k < suffixes.size(); ++k) {
        KeyEntry entry;
        entry.suffix = suffixes[k];
        uint64_t base_pv = 1000000 + (rng() % 1000000);
        uint32_t base_id = 100 + (rng() % 1000);
        uint64_t pv = base_pv;
        for (int v = 0; v < versions_per_key; ++v) {
            entry.packed_versions.push_back(pv);
            entry.ids.push_back(base_id + (rng() % 10));
            pv -= (rng() % 100 + 1);  // desc-sorted, safe decrement
        }
        contents.keys.push_back(std::move(entry));
    }
    return contents;
}

// ============================================================
// Benchmark runner
// ============================================================

struct BenchResult {
    const char* scenario;
    int num_keys;
    int versions_per_key;
    size_t row_bits;
    size_t columnar_bits;
    double encode_row_ns;
    double encode_columnar_ns;
    double decode_row_ns;
    double decode_columnar_ns;
    double targeted_row_ns;       // decodeKeyAt last key (row-oriented)
    double targeted_columnar_ns;  // decodeKeyAt last key (columnar)
};

static const uint8_t kSuffixBits = 44;  // 20-bit bucket index
static const uint32_t kBucketBytes = 4096;

static BenchResult runBench(const char* scenario,
                            const BucketContents& contents,
                            int num_keys, int versions_per_key) {
    BenchResult r{};
    r.scenario = scenario;
    r.num_keys = num_keys;
    r.versions_per_key = versions_per_key;

    BucketCodec<RowLayout> row_codec(kSuffixBits, kBucketBytes);
    BucketCodec<ColumnarLayout> col_codec(kSuffixBits, kBucketBytes);

    std::vector<uint8_t> row_buf(kBucketBytes, 0);
    Bucket row_bucket;
    row_bucket.data = row_buf.data();

    std::vector<uint8_t> col_buf(kBucketBytes, 0);
    Bucket col_bucket;
    col_bucket.data = col_buf.data();

    constexpr int kRuns = 1000;

    // Row encode.
    {
        auto start = std::chrono::high_resolution_clock::now();
        size_t bits = 0;
        for (int i = 0; i < kRuns; ++i) {
            bits = row_codec.encodeBucket(row_bucket, contents);
        }
        auto end = std::chrono::high_resolution_clock::now();
        r.row_bits = bits;
        r.encode_row_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count() /
            static_cast<double>(kRuns);
    }

    // Row decode.
    {
        row_codec.encodeBucket(row_bucket, contents);
        auto start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < kRuns; ++i) {
            auto decoded = row_codec.decodeBucket(row_bucket);
            (void)decoded;
        }
        auto end = std::chrono::high_resolution_clock::now();
        r.decode_row_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count() /
            static_cast<double>(kRuns);
    }

    // Columnar encode.
    {
        auto start = std::chrono::high_resolution_clock::now();
        size_t bits = 0;
        for (int i = 0; i < kRuns; ++i) {
            bits = col_codec.encodeBucket(col_bucket, contents);
        }
        auto end = std::chrono::high_resolution_clock::now();
        r.columnar_bits = bits;
        r.encode_columnar_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count() /
            static_cast<double>(kRuns);
    }

    // Columnar decode.
    {
        col_codec.encodeBucket(col_bucket, contents);
        auto start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < kRuns; ++i) {
            auto decoded = col_codec.decodeBucket(col_bucket);
            (void)decoded;
        }
        auto end = std::chrono::high_resolution_clock::now();
        r.decode_columnar_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count() /
            static_cast<double>(kRuns);
    }

    // Targeted decode: decode the last key only (worst case for skip-based).
    uint16_t target_idx = static_cast<uint16_t>(contents.keys.size() - 1);
    uint64_t target_suffix = contents.keys[target_idx].suffix;

    // Row targeted decode (includes decodeSuffixes to match real-world usage).
    {
        row_codec.encodeBucket(row_bucket, contents);
        auto start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < kRuns; ++i) {
            auto scan = row_codec.decodeSuffixes(row_bucket);
            auto key = row_codec.decodeKeyAt(row_bucket, target_idx, target_suffix,
                                              scan.data_start_bit);
            (void)key;
        }
        auto end = std::chrono::high_resolution_clock::now();
        r.targeted_row_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count() /
            static_cast<double>(kRuns);
    }

    // Columnar targeted decode (includes decodeSuffixes to match real-world usage).
    {
        col_codec.encodeBucket(col_bucket, contents);
        auto start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < kRuns; ++i) {
            auto scan = col_codec.decodeSuffixes(col_bucket);
            auto key = col_codec.decodeKeyAt(col_bucket, target_idx, target_suffix,
                                              scan.data_start_bit);
            (void)key;
        }
        auto end = std::chrono::high_resolution_clock::now();
        r.targeted_columnar_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count() /
            static_cast<double>(kRuns);
    }

    return r;
}

// ============================================================
// Benchmark tests
// ============================================================

class BucketCodecBench : public ::testing::Test {
protected:
    std::vector<BenchResult> results;

    void TearDown() override {
        if (!results.empty()) {
            printf("\n%-12s %4s %4s | %8s %8s %6s"
                   " | %8s %8s %5s"
                   " | %8s %8s %5s"
                   " | %8s %8s %5s\n",
                   "Scenario", "Keys", "V/K",
                   "Row(b)", "Col(b)", "Saved",
                   "EncR", "EncC", "Ratio",
                   "DecR", "DecC", "Ratio",
                   "TgtR", "TgtC", "Ratio");
            for (size_t i = 0; i < 120; ++i) putchar('-');
            putchar('\n');
            for (const auto& r : results) {
                double saving = r.row_bits > 0
                    ? 100.0 * (1.0 - static_cast<double>(r.columnar_bits) /
                                      static_cast<double>(r.row_bits))
                    : 0.0;
                double enc_ratio = r.encode_row_ns > 0
                    ? r.encode_columnar_ns / r.encode_row_ns : 0.0;
                double dec_ratio = r.decode_row_ns > 0
                    ? r.decode_columnar_ns / r.decode_row_ns : 0.0;
                double tgt_ratio = r.targeted_row_ns > 0
                    ? r.targeted_columnar_ns / r.targeted_row_ns : 0.0;
                printf("%-12s %4d %4d | %8zu %8zu %5.1f%%"
                       " | %7.0fns %7.0fns %4.2fx"
                       " | %7.0fns %7.0fns %4.2fx"
                       " | %7.0fns %7.0fns %4.2fx\n",
                       r.scenario, r.num_keys, r.versions_per_key,
                       r.row_bits, r.columnar_bits, saving,
                       r.encode_row_ns, r.encode_columnar_ns, enc_ratio,
                       r.decode_row_ns, r.decode_columnar_ns, dec_ratio,
                       r.targeted_row_ns, r.targeted_columnar_ns, tgt_ratio);
            }
            printf("\n");
        }
    }
};

// Latency bound: columnar should not be more than 10x slower than row.
// Generous threshold to avoid flaky failures from system jitter; empirically
// the ratio is ~1.0-2.5x for encode/decode and ~1.0-3.0x for targeted decode.
// Under parallel ctest or heavy system load, 4x was insufficient.
static constexpr double kMaxLatencyRatio = 10.0;

static void checkLatency(const BenchResult& r, int nk, int nv) {
    if (r.encode_row_ns > 100) {  // skip noise for sub-100ns operations
        EXPECT_LT(r.encode_columnar_ns, r.encode_row_ns * kMaxLatencyRatio)
            << "Columnar encode too slow (keys=" << nk << ", v/key=" << nv << ")";
    }
    if (r.decode_row_ns > 100) {
        EXPECT_LT(r.decode_columnar_ns, r.decode_row_ns * kMaxLatencyRatio)
            << "Columnar decode too slow (keys=" << nk << ", v/key=" << nv << ")";
    }
    if (r.targeted_row_ns > 100) {
        EXPECT_LT(r.targeted_columnar_ns, r.targeted_row_ns * kMaxLatencyRatio)
            << "Columnar targeted decode too slow (keys=" << nk << ", v/key=" << nv << ")";
    }
}

TEST_F(BucketCodecBench, SequentialVersions) {
    for (int nk : {1, 5, 20, 50}) {
        for (int nv : {1, 3, 10}) {
            auto contents = makeSequential(nk, nv);
            auto r = runBench("sequential", contents, nk, nv);
            EXPECT_LE(r.columnar_bits, r.row_bits)
                << "Columnar should be <= row-oriented for sequential"
                << " (keys=" << nk << ", v/key=" << nv << ")";
            checkLatency(r, nk, nv);
            results.push_back(r);
        }
    }
}

TEST_F(BucketCodecBench, ClusteredVersions) {
    for (int nk : {1, 5, 20, 50}) {
        for (int nv : {1, 3, 10}) {
            auto contents = makeClustered(nk, nv);
            auto r = runBench("clustered", contents, nk, nv);
            EXPECT_LE(r.columnar_bits, r.row_bits)
                << "Columnar should be <= row-oriented for clustered"
                << " (keys=" << nk << ", v/key=" << nv << ")";
            checkLatency(r, nk, nv);
            results.push_back(r);
        }
    }
}

TEST_F(BucketCodecBench, RandomVersions) {
    for (int nk : {1, 5, 20, 50}) {
        for (int nv : {1, 3, 10}) {
            auto contents = makeRandom(nk, nv, 42);
            int actual_keys = static_cast<int>(contents.keys.size());
            auto r = runBench("random", contents, actual_keys, nv);
            // Random data may not compress well — allow small regression.
            EXPECT_LE(r.columnar_bits, r.row_bits + 10)
                << "Columnar should not be significantly worse for random"
                << " (keys=" << actual_keys << ", v/key=" << nv << ")";
            checkLatency(r, actual_keys, nv);
            results.push_back(r);
        }
    }
}

TEST_F(BucketCodecBench, SingleKeyManyVersions) {
    auto contents = makeSequential(1, 100);
    auto r = runBench("1key-100v", contents, 1, 100);
    // Single key: no cross-key deltas, should be same or marginally worse
    // (counts column adds gamma(100) ≈ 13 bits overhead).
    EXPECT_LE(r.columnar_bits, r.row_bits + 20);
    checkLatency(r, 1, 100);
    results.push_back(r);
}

// Round-trip correctness: verify columnar decode matches row decode.
TEST(BucketCodecBenchCorrectness, RoundTripMatchesRow) {
    for (int nk : {1, 5, 20}) {
        for (int nv : {1, 3, 10}) {
            auto contents = makeClustered(nk, nv);

            BucketCodec<ColumnarLayout> codec(kSuffixBits, kBucketBytes);
            Bucket bucket;
            std::vector<uint8_t> buf(kBucketBytes, 0);
            bucket.data = buf.data();
            codec.encodeBucket(bucket, contents);

            auto decoded = codec.decodeBucket(bucket);
            ASSERT_EQ(decoded.keys.size(), contents.keys.size())
                << "Key count mismatch (keys=" << nk << ", v/key=" << nv << ")";

            for (size_t k = 0; k < contents.keys.size(); ++k) {
                EXPECT_EQ(decoded.keys[k].suffix, contents.keys[k].suffix);
                ASSERT_EQ(decoded.keys[k].packed_versions.size(),
                          contents.keys[k].packed_versions.size());
                for (size_t v = 0; v < contents.keys[k].packed_versions.size(); ++v) {
                    EXPECT_EQ(decoded.keys[k].packed_versions[v],
                              contents.keys[k].packed_versions[v])
                        << "pv mismatch at key=" << k << " v=" << v;
                    EXPECT_EQ(decoded.keys[k].ids[v], contents.keys[k].ids[v])
                        << "id mismatch at key=" << k << " v=" << v;
                }
            }
        }
    }
}
