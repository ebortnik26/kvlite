// Stress test: write to a small key set with rotating snapshots.
// Parameterized across three dedup modes:
//   Inline  — dedup_on_put=true, prune daemon off
//   Daemon  — dedup_on_put=false, prune daemon every 1s
//   Neither — dedup_on_put=false, prune daemon off (GC-only baseline)

#include <gtest/gtest.h>
#include <kvlite/kvlite.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <random>
#include <thread>

namespace fs = std::filesystem;

// Inline     — flush dedup on, daemon off, GC on (baseline: flush handles it)
// Daemon     — flush dedup off, daemon on (1s), GC on
// DaemonOnly — flush dedup off, daemon on (1s), GC DISABLED
//              (proves the daemon alone handles dedup — no help from flush or GC)
// Neither    — flush dedup off, daemon off, GC on (GC does all the work)
enum class DedupMode { Inline, Daemon, DaemonOnly, Neither };

static const char* modeName(DedupMode m) {
    switch (m) {
        case DedupMode::Inline:     return "Inline";
        case DedupMode::Daemon:     return "Daemon";
        case DedupMode::DaemonOnly: return "DaemonOnly";
        case DedupMode::Neither:    return "Neither";
    }
    return "?";
}

class VersionDedupTest : public ::testing::TestWithParam<DedupMode> {};

TEST_P(VersionDedupTest, RotatingSnapshotsLimitVersions) {
    DedupMode mode = GetParam();

    fs::path test_dir = fs::temp_directory_path() / "kvlite_dedup_test";
    fs::remove_all(test_dir);

    kvlite::DB db;
    kvlite::Options opts;
    opts.create_if_missing = true;
    opts.memtable_size = 1 * 1024 * 1024;  // 1MB — forces frequent flushes
    opts.gc_threshold = 0.1;
    opts.savepoint_interval_sec = 5;
    opts.dedup_on_put = (mode == DedupMode::Inline);
    // DaemonOnly disables GC entirely so the daemon is the sole deduper.
    if (mode == DedupMode::DaemonOnly) {
        opts.gc_policy = kvlite::GCPolicy::MANUAL;  // no auto-GC
        opts.gc_interval_sec = 0;
    } else {
        opts.gc_interval_sec = 2;
    }
    opts.version_prune_interval_sec =
        (mode == DedupMode::Daemon || mode == DedupMode::DaemonOnly) ? 1 : 0;
    ASSERT_TRUE(db.open(test_dir.string(), opts).ok());

    const int kNumKeys = 256;
    const auto kDuration = std::chrono::seconds(30);
    const auto kSnapshotInterval = std::chrono::milliseconds(100);

    std::atomic<bool> stop{false};
    std::atomic<uint64_t> write_count{0};

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

    std::thread snapshotter([&] {
        while (!stop.load(std::memory_order_relaxed)) {
            auto snap = db.createSnapshot();
            std::this_thread::sleep_for(kSnapshotInterval);
            db.releaseSnapshot(snap);
        }
    });

    auto start = std::chrono::steady_clock::now();
    while (std::chrono::steady_clock::now() - start < kDuration) {
        std::this_thread::sleep_for(std::chrono::seconds(10));

        kvlite::DBStats stats;
        ASSERT_TRUE(db.getStats(stats).ok());

        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::steady_clock::now() - start).count();

        std::cout << "[" << elapsed << "s] "
                  << "mode=" << modeName(mode)
                  << "  writes=" << write_count.load()
                  << "  live=" << stats.num_live_entries
                  << "  hist=" << stats.num_historical_entries
                  << "  segs=" << stats.num_log_files
                  << "  gc=" << stats.gc_count
                  << "  flush=" << stats.flush_count
                  << "  prune=" << stats.prune_count
                  << std::endl;
    }

    stop.store(true, std::memory_order_relaxed);
    writer.join();
    snapshotter.join();

    kvlite::DBStats stats;
    ASSERT_TRUE(db.getStats(stats).ok());

    uint64_t total_entries = stats.num_live_entries + stats.num_historical_entries;
    double versions_per_key = static_cast<double>(total_entries) / kNumKeys;

    std::cout << "\n=== FINAL (mode=" << modeName(mode) << ") ===\n"
              << "Total writes:      " << write_count.load() << "\n"
              << "Live entries:      " << stats.num_live_entries << "\n"
              << "Historical entries: " << stats.num_historical_entries << "\n"
              << "Versions per key:  " << versions_per_key << "\n"
              << "Segments:          " << stats.num_log_files << "\n"
              << "GC runs:           " << stats.gc_count << "\n"
              << "Flushes:           " << stats.flush_count << "\n"
              << "Prune runs:        " << stats.prune_count << "\n"
              << std::endl;

    if (mode == DedupMode::Inline || mode == DedupMode::Daemon ||
        mode == DedupMode::DaemonOnly) {
        EXPECT_LT(versions_per_key, 10.0)
            << modeName(mode) << " dedup should bound versions per key";
    }

    EXPECT_LT(total_entries, write_count.load() / 10)
        << "GlobalIndex not deduplicating — entry count proportional to writes";

    db.close();
    fs::remove_all(test_dir);
}

INSTANTIATE_TEST_SUITE_P(
    DedupModes, VersionDedupTest,
    ::testing::Values(DedupMode::Inline, DedupMode::Daemon,
                      DedupMode::DaemonOnly, DedupMode::Neither),
    [](const ::testing::TestParamInfo<DedupMode>& info) {
        return modeName(info.param);
    });
