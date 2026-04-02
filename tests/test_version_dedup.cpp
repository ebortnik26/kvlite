// Stress test: write to a small key set with rotating snapshots.
// Parameterized on dedup_on_put — verifies that inline dedup bounds
// versions per key, and that the system is stable without it.

#include <gtest/gtest.h>
#include <kvlite/kvlite.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <random>
#include <thread>

namespace fs = std::filesystem;

class VersionDedupTest : public ::testing::TestWithParam<bool> {};

TEST_P(VersionDedupTest, RotatingSnapshotsLimitVersions) {
    bool dedup_on_put = GetParam();

    fs::path test_dir = fs::temp_directory_path() / "kvlite_dedup_test";
    fs::remove_all(test_dir);

    kvlite::DB db;
    kvlite::Options opts;
    opts.create_if_missing = true;
    opts.memtable_size = 1 * 1024 * 1024;  // 1MB — forces frequent flushes
    opts.gc_interval_sec = 2;               // aggressive GC
    opts.gc_threshold = 0.1;                // low threshold
    opts.savepoint_interval_sec = 5;
    opts.dedup_on_put = dedup_on_put;
    ASSERT_TRUE(db.open(test_dir.string(), opts).ok());

    const int kNumKeys = 256;
    const auto kDuration = std::chrono::seconds(30);
    const auto kSnapshotInterval = std::chrono::milliseconds(100);

    std::atomic<bool> stop{false};
    std::atomic<uint64_t> write_count{0};

    // Writer thread: hammer 256 keys with random updates.
    std::thread writer([&] {
        std::mt19937 rng(42);
        std::uniform_int_distribution<int> key_dist(0, kNumKeys - 1);
        uint64_t seq = 0;
        while (!stop.load(std::memory_order_relaxed)) {
            int k = key_dist(rng);
            std::string key = "k_" + std::to_string(k);
            std::string val = "v_" + std::to_string(++seq);
            auto s = db.put(key, val);
            if (!s.ok()) break;
            write_count.fetch_add(1, std::memory_order_relaxed);
        }
    });

    // Snapshot thread: create and release a snapshot every 100ms.
    std::thread snapshotter([&] {
        while (!stop.load(std::memory_order_relaxed)) {
            auto snap = db.createSnapshot();
            std::this_thread::sleep_for(kSnapshotInterval);
            db.releaseSnapshot(snap);
        }
    });

    // Run for the specified duration, printing stats every 10s.
    auto start = std::chrono::steady_clock::now();
    while (std::chrono::steady_clock::now() - start < kDuration) {
        std::this_thread::sleep_for(std::chrono::seconds(10));

        kvlite::DBStats stats;
        ASSERT_TRUE(db.getStats(stats).ok());

        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::steady_clock::now() - start).count();

        std::cout << "[" << elapsed << "s] "
                  << "dedup=" << dedup_on_put
                  << "  writes=" << write_count.load()
                  << "  live=" << stats.num_live_entries
                  << "  hist=" << stats.num_historical_entries
                  << "  segs=" << stats.num_log_files
                  << "  gc=" << stats.gc_count
                  << "  flush=" << stats.flush_count
                  << std::endl;
    }

    stop.store(true, std::memory_order_relaxed);
    writer.join();
    snapshotter.join();

    // Final stats.
    kvlite::DBStats stats;
    ASSERT_TRUE(db.getStats(stats).ok());

    uint64_t total_entries = stats.num_live_entries + stats.num_historical_entries;
    double versions_per_key = static_cast<double>(total_entries) / kNumKeys;

    std::cout << "\n=== FINAL (dedup_on_put=" << dedup_on_put << ") ===\n"
              << "Total writes:      " << write_count.load() << "\n"
              << "Live entries:      " << stats.num_live_entries << "\n"
              << "Historical entries: " << stats.num_historical_entries << "\n"
              << "Versions per key:  " << versions_per_key << "\n"
              << "Segments:          " << stats.num_log_files << "\n"
              << "GC runs:           " << stats.gc_count << "\n"
              << "Flushes:           " << stats.flush_count << "\n"
              << std::endl;

    if (dedup_on_put) {
        // With inline dedup: versions per key bounded by snapshot count + slack.
        EXPECT_LT(versions_per_key, 10.0)
            << "dedup_on_put should bound versions per key";
    }

    // Both modes: total entries much less than total writes.
    EXPECT_LT(total_entries, write_count.load() / 10)
        << "GlobalIndex not deduplicating — entry count proportional to writes";

    db.close();
    fs::remove_all(test_dir);
}

INSTANTIATE_TEST_SUITE_P(
    DedupOnPut, VersionDedupTest,
    ::testing::Values(true, false),
    [](const ::testing::TestParamInfo<bool>& info) {
        return info.param ? "DedupEnabled" : "DedupDisabled";
    });
