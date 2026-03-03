#include <gtest/gtest.h>

#include <cstdint>
#include <random>
#include <set>
#include <vector>

#include "zipfian_generator.h"

TEST(ZipfianTest, AllValuesInRange) {
    constexpr uint64_t kNumKeys = 10000;
    constexpr int kSamples = 100000;

    ZipfianGenerator gen(kNumKeys);
    std::mt19937_64 rng(12345);

    for (int i = 0; i < kSamples; ++i) {
        uint64_t v = gen(rng);
        ASSERT_LT(v, kNumKeys) << "sample " << i;
    }
}

TEST(ZipfianTest, RawDistributionIsSkewed) {
    constexpr uint64_t kNumKeys = 10000;
    constexpr int kSamples = 1000000;

    ZipfianGenerator gen(kNumKeys);
    std::mt19937_64 rng(42);

    std::vector<uint64_t> counts(kNumKeys, 0);
    for (int i = 0; i < kSamples; ++i) {
        uint64_t v = gen.nextRaw(rng);
        ASSERT_LT(v, kNumKeys);
        counts[v]++;
    }

    // Rank 0 should be the most popular
    for (uint64_t i = 1; i < kNumKeys; ++i) {
        EXPECT_GE(counts[0], counts[i])
            << "rank 0 should be >= rank " << i;
    }

    // Top 1% of ranks should get >20% of samples
    uint64_t top1_count = 0;
    uint64_t top1_ranks = kNumKeys / 100;
    if (top1_ranks == 0) top1_ranks = 1;
    for (uint64_t i = 0; i < top1_ranks; ++i) {
        top1_count += counts[i];
    }
    double top1_fraction = static_cast<double>(top1_count) / kSamples;
    EXPECT_GT(top1_fraction, 0.20)
        << "top 1% of ranks should get >20% of samples, got "
        << (top1_fraction * 100) << "%";
}

TEST(ZipfianTest, ScrambledCoversKeySpace) {
    // Verify that scrambling disperses hot keys across the key space rather
    // than clustering them near zero.  With theta=0.99, the Zipfian tail
    // is very cold so perfect coverage isn't expected; we check that >60%
    // of keys are hit and that the hot keys aren't bunched at the start.
    constexpr uint64_t kNumKeys = 1000;
    constexpr int kSamples = 500000;

    ZipfianGenerator gen(kNumKeys);
    std::mt19937_64 rng(99);

    std::vector<uint64_t> counts(kNumKeys, 0);
    for (int i = 0; i < kSamples; ++i) {
        counts[gen(rng)]++;
    }

    uint64_t covered = 0;
    for (uint64_t i = 0; i < kNumKeys; ++i) {
        if (counts[i] > 0) ++covered;
    }

    double coverage = static_cast<double>(covered) / kNumKeys;
    EXPECT_GT(coverage, 0.60)
        << "scrambled zipfian should cover >60% of key space, got "
        << (coverage * 100) << "% (" << covered << "/" << kNumKeys << ")";

    // Verify hot keys are spread out: the top-10 keys by count should NOT
    // all be in the first 10% of the key space (they would be without scrambling).
    std::vector<std::pair<uint64_t, uint64_t>> key_counts;
    for (uint64_t i = 0; i < kNumKeys; ++i) {
        if (counts[i] > 0) key_counts.push_back({counts[i], i});
    }
    std::sort(key_counts.rbegin(), key_counts.rend());

    int in_first_tenth = 0;
    int top_n = std::min(10, static_cast<int>(key_counts.size()));
    for (int i = 0; i < top_n; ++i) {
        if (key_counts[static_cast<size_t>(i)].second < kNumKeys / 10) {
            ++in_first_tenth;
        }
    }
    EXPECT_LT(in_first_tenth, top_n)
        << "scrambling should spread hot keys; all top-" << top_n
        << " are in first 10%";
}

TEST(ZipfianTest, Reproducible) {
    constexpr uint64_t kNumKeys = 10000;
    constexpr int kSamples = 1000;

    ZipfianGenerator gen1(kNumKeys);
    ZipfianGenerator gen2(kNumKeys);
    std::mt19937_64 rng1(777);
    std::mt19937_64 rng2(777);

    for (int i = 0; i < kSamples; ++i) {
        ASSERT_EQ(gen1(rng1), gen2(rng2)) << "mismatch at sample " << i;
    }
}

TEST(ZipfianTest, SingleItem) {
    ZipfianGenerator gen(1);
    std::mt19937_64 rng(0);

    for (int i = 0; i < 1000; ++i) {
        ASSERT_EQ(gen(rng), 0u);
    }
}
