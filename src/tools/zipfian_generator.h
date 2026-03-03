#pragma once

#include <cmath>
#include <cstdint>
#include <cstring>
#include <random>

// Scrambled Zipfian generator following the YCSB algorithm.
// Based on the Yahoo! Cloud Serving Benchmark (YCSB):
//   B. F. Cooper et al., "Benchmarking Cloud Serving Systems with YCSB",
//   SoCC 2010.  https://github.com/brianfrankcooper/YCSB
//
// Produces values in [0, num_items) with Zipfian skew (theta ≈ 0.99),
// then scrambles via FNV-1a hash to scatter hot keys across the key space.

class ZipfianGenerator {
public:
    explicit ZipfianGenerator(uint64_t num_items, double theta = 0.99)
        : num_items_(num_items), theta_(theta) {
        zeta_n_ = zetaStatic(num_items_, theta_);
        zeta_2_ = zetaStatic(2, theta_);
        alpha_ = 1.0 / (1.0 - theta_);
        eta_ = (1.0 - std::pow(2.0 / static_cast<double>(num_items_),
                                1.0 - theta_)) /
               (1.0 - zeta_2_ / zeta_n_);
    }

    // Un-scrambled Zipfian value via Gray et al. formula.
    template <typename RNG>
    uint64_t nextRaw(RNG& rng) {
        std::uniform_real_distribution<double> udist(0.0, 1.0);
        double u = udist(rng);
        double uz = u * zeta_n_;

        if (uz < 1.0) return 0;
        if (uz < 1.0 + std::pow(0.5, theta_)) return 1;

        return static_cast<uint64_t>(
            static_cast<double>(num_items_) *
            std::pow(eta_ * u - eta_ + 1.0, alpha_));
    }

    // Scrambled Zipfian: hash the raw rank to scatter hot keys.
    template <typename RNG>
    uint64_t operator()(RNG& rng) {
        return fnvHash64(nextRaw(rng)) % num_items_;
    }

    static uint64_t fnvHash64(uint64_t val) {
        constexpr uint64_t kFNVOffsetBasis = 14695981039346656037ULL;
        constexpr uint64_t kFNVPrime = 1099511628211ULL;

        uint8_t bytes[8];
        std::memcpy(bytes, &val, 8);

        uint64_t hash = kFNVOffsetBasis;
        for (int i = 0; i < 8; ++i) {
            hash ^= bytes[i];
            hash *= kFNVPrime;
        }
        return hash;
    }

private:
    static double zetaStatic(uint64_t n, double theta) {
        double sum = 0.0;
        for (uint64_t i = 0; i < n; ++i) {
            sum += 1.0 / std::pow(static_cast<double>(i + 1), theta);
        }
        return sum;
    }

    uint64_t num_items_;
    double theta_;
    double zeta_n_;
    double zeta_2_;
    double alpha_;
    double eta_;
};
