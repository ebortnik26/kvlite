#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <thread>
#include <vector>

#include "internal/global_index.h"
#include "internal/manifest.h"
#include "internal/read_write_delta_hash_table.h"

using namespace kvlite::internal;
using kvlite::Status;

static uint64_t H(const std::string& s) {
    return dhtHashBytes(s.data(), s.size());
}

// ---------------------------------------------------------------------------
// Fixture: creates a temp dir + Manifest + GlobalIndex per test.
// ---------------------------------------------------------------------------
class SavepointBench : public ::testing::Test {
protected:
    void SetUp() override {
        db_dir_ = ::testing::TempDir() + "/sp_bench_" +
                  std::to_string(reinterpret_cast<uintptr_t>(this));
        std::filesystem::create_directories(db_dir_);
        ASSERT_TRUE(manifest_.create(db_dir_).ok());
        index_ = std::make_unique<GlobalIndex>(manifest_);
        GlobalIndex::Options opts;
        ASSERT_TRUE(index_->open(db_dir_, opts).ok());
    }

    void TearDown() override {
        if (index_ && index_->isOpen()) index_->close();
        index_.reset();
        manifest_.close();
        std::filesystem::remove_all(db_dir_);
    }

    std::string db_dir_;
    Manifest manifest_;
    std::unique_ptr<GlobalIndex> index_;
};

// ---------------------------------------------------------------------------
// SnapshotBucketChain: per-bucket spinlock + copy over 1M buckets (100K keys)
// ---------------------------------------------------------------------------
TEST_F(SavepointBench, SnapshotBucketChain) {
    const int kNumKeys = 100'000;
    for (int i = 0; i < kNumKeys; ++i) {
        std::string key = "snapbk_" + std::to_string(i);
        ASSERT_TRUE(index_->put(H(key), i + 1, i + 1).ok());
    }

    // Access the underlying DHT via a second instance that shares the same
    // bucket layout. We measure snapshotBucketChain on a standalone RW-DHT.
    ReadWriteDeltaHashTable dht;
    const uint32_t nBuckets = dht.numBuckets();

    // Populate the DHT directly.
    for (int i = 0; i < kNumKeys; ++i) {
        std::string key = "snapbk_" + std::to_string(i);
        dht.addEntry(H(key), i + 1, i + 1);
    }

    std::vector<uint8_t> buf;
    auto t0 = std::chrono::steady_clock::now();
    for (uint32_t bi = 0; bi < nBuckets; ++bi) {
        dht.snapshotBucketChain(bi, buf);
    }
    auto t1 = std::chrono::steady_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

    std::cout << "[BENCH] SnapshotBucketChain: " << ms << " ms ("
              << nBuckets << " buckets, " << kNumKeys << " keys)\n";
    EXPECT_LT(ms, 200.0) << "Regression: bucket snapshot too slow";
}

// ---------------------------------------------------------------------------
// StagePutThroughput: atomic CAS + DHT insert via stagePut (100K ops)
// ---------------------------------------------------------------------------
TEST_F(SavepointBench, StagePutThroughput) {
    const int kNumOps = 100'000;

    auto t0 = std::chrono::steady_clock::now();
    for (int i = 0; i < kNumOps; ++i) {
        std::string key = "stageput_" + std::to_string(i);
        ASSERT_TRUE(index_->stagePut(H(key), i + 1, i + 1).ok());
    }
    auto t1 = std::chrono::steady_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

    // Commit to keep WAL state consistent.
    ASSERT_TRUE(index_->commitWB(kNumOps).ok());

    std::cout << "[BENCH] StagePutThroughput: " << ms << " ms ("
              << kNumOps << " ops)\n";
    EXPECT_LT(ms, 500.0) << "Regression: stagePut too slow";
}

// ---------------------------------------------------------------------------
// SavepointWriteLatency: full savepoint write at 100K keys
// ---------------------------------------------------------------------------
TEST_F(SavepointBench, SavepointWriteLatency) {
    const int kNumKeys = 100'000;
    for (int i = 0; i < kNumKeys; ++i) {
        std::string key = "spwrite_" + std::to_string(i);
        ASSERT_TRUE(index_->put(H(key), i + 1, i + 1).ok());
    }

    auto t0 = std::chrono::steady_clock::now();
    ASSERT_TRUE(index_->storeSavepoint(0).ok());
    auto t1 = std::chrono::steady_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

    std::cout << "[BENCH] SavepointWriteLatency: " << ms << " ms ("
              << kNumKeys << " keys)\n";
    EXPECT_LT(ms, 2000.0) << "Regression: savepoint write too slow";
}

// ---------------------------------------------------------------------------
// ConcurrentFlushAndSavepoint: ratio of concurrent vs baseline savepoint time.
// This is the key regression detector — if exclusive locking is accidentally
// re-introduced, the ratio will blow up.
// ---------------------------------------------------------------------------
TEST_F(SavepointBench, ConcurrentFlushAndSavepoint) {
    const int kNumKeys = 100'000;
    for (int i = 0; i < kNumKeys; ++i) {
        std::string key = "conc_" + std::to_string(i);
        ASSERT_TRUE(index_->put(H(key), i + 1, i + 1).ok());
    }

    // --- Baseline: savepoint with no concurrent writers ---
    auto t0 = std::chrono::steady_clock::now();
    ASSERT_TRUE(index_->storeSavepoint(0).ok());
    auto t1 = std::chrono::steady_clock::now();
    double baseline_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

    // --- Concurrent: savepoint while flush (stagePut) runs in parallel ---
    const int kFlushOps = 50'000;
    std::atomic<bool> flush_done{false};
    std::thread flush_thread([&]() {
        for (int i = 0; i < kFlushOps; ++i) {
            std::string key = "flush_" + std::to_string(i);
            auto s = index_->stagePut(H(key), kNumKeys + i + 1, kNumKeys + i + 1);
            if (!s.ok()) break;
        }
        flush_done.store(true);
    });

    auto t2 = std::chrono::steady_clock::now();
    ASSERT_TRUE(index_->storeSavepoint(0).ok());
    auto t3 = std::chrono::steady_clock::now();
    double concurrent_ms = std::chrono::duration<double, std::milli>(t3 - t2).count();

    flush_thread.join();
    // Commit the staged entries.
    ASSERT_TRUE(index_->commitWB(kNumKeys + kFlushOps).ok());

    double ratio = concurrent_ms / std::max(baseline_ms, 1.0);
    std::cout << "[BENCH] ConcurrentFlushAndSavepoint: baseline=" << baseline_ms
              << " ms, concurrent=" << concurrent_ms << " ms, ratio=" << ratio << "x\n";
    EXPECT_LT(ratio, 3.0)
        << "Regression: concurrent savepoint too slow relative to baseline "
        << "(suggests exclusive locking regression)";
}

// ---------------------------------------------------------------------------
// LoadBucketChain: read-path bucket loading (1M buckets, 100K keys)
// ---------------------------------------------------------------------------
TEST_F(SavepointBench, LoadBucketChain) {
    const int kNumKeys = 100'000;

    ReadWriteDeltaHashTable src;
    for (int i = 0; i < kNumKeys; ++i) {
        std::string key = "loadbk_" + std::to_string(i);
        src.addEntry(H(key), i + 1, i + 1);
    }

    const uint32_t nBuckets = src.numBuckets();

    // Snapshot all chains.
    struct ChainData {
        std::vector<uint8_t> data;
        uint8_t chain_len;
    };
    std::vector<ChainData> chains(nBuckets);
    for (uint32_t bi = 0; bi < nBuckets; ++bi) {
        chains[bi].chain_len = static_cast<uint8_t>(
            src.snapshotBucketChain(bi, chains[bi].data));
    }

    // Load into a fresh DHT and time it.
    ReadWriteDeltaHashTable dst;

    auto t0 = std::chrono::steady_clock::now();
    for (uint32_t bi = 0; bi < nBuckets; ++bi) {
        if (chains[bi].chain_len > 0) {
            dst.loadBucketChain(bi, chains[bi].data.data(), chains[bi].chain_len);
        }
    }
    auto t1 = std::chrono::steady_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

    std::cout << "[BENCH] LoadBucketChain: " << ms << " ms ("
              << nBuckets << " buckets, " << kNumKeys << " keys)\n";
    EXPECT_LT(ms, 200.0) << "Regression: bucket chain loading too slow";
}
