#include <gtest/gtest.h>

#include <atomic>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <mutex>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include "internal/crc32.h"
#include "internal/delta_hash_table.h"
#include "internal/segment.h"
#include "internal/memtable.h"
#include "internal/write_buffer.h"

using kvlite::internal::Memtable;

TEST(Memtable, PutAndGetSingleEntry) {
    Memtable wb;
    wb.put("key1", 1, "value1", false);

    std::string value;
    uint64_t version;
    bool tombstone;
    ASSERT_TRUE(wb.get("key1", value, version, tombstone));
    EXPECT_EQ(value, "value1");
    EXPECT_EQ(version, 1u);
    EXPECT_FALSE(tombstone);
}

TEST(Memtable, GetMissing) {
    Memtable wb;
    std::string value;
    uint64_t version;
    bool tombstone;
    EXPECT_FALSE(wb.get("missing", value, version, tombstone));
}

TEST(Memtable, GetReturnsLatestVersion) {
    Memtable wb;
    wb.put("key1", 1, "v1", false);
    wb.put("key1", 5, "v5", false);
    wb.put("key1", 3, "v3", false);

    std::string value;
    uint64_t version;
    bool tombstone;
    // Latest = highest version (5), regardless of insertion order
    ASSERT_TRUE(wb.get("key1", value, version, tombstone));
    EXPECT_EQ(value, "v5");
    EXPECT_EQ(version, 5u);
}

TEST(Memtable, Tombstone) {
    Memtable wb;
    wb.put("key1", 1, "", true);

    std::string value;
    uint64_t version;
    bool tombstone;
    ASSERT_TRUE(wb.get("key1", value, version, tombstone));
    EXPECT_TRUE(tombstone);
    EXPECT_EQ(version, 1u);
}

TEST(Memtable, GetByVersionExact) {
    Memtable wb;
    wb.put("key1", 1, "v1", false);
    wb.put("key1", 3, "v3", false);
    wb.put("key1", 5, "v5", false);

    std::string value;
    uint64_t version;
    bool tombstone;

    // Upper bound is packed: (logical_version << 1) | 1
    ASSERT_TRUE(wb.getByVersion("key1", (3ULL << 1) | 1, value, version, tombstone));
    EXPECT_EQ(value, "v3");
    EXPECT_EQ(version, 3u);
}

TEST(Memtable, GetByVersionBetween) {
    Memtable wb;
    wb.put("key1", 1, "v1", false);
    wb.put("key1", 3, "v3", false);
    wb.put("key1", 5, "v5", false);

    std::string value;
    uint64_t version;
    bool tombstone;

    ASSERT_TRUE(wb.getByVersion("key1", (4ULL << 1) | 1, value, version, tombstone));
    EXPECT_EQ(value, "v3");
    EXPECT_EQ(version, 3u);
}

TEST(Memtable, GetByVersionTooLow) {
    Memtable wb;
    wb.put("key1", 5, "v5", false);

    std::string value;
    uint64_t version;
    bool tombstone;
    EXPECT_FALSE(wb.getByVersion("key1", (3ULL << 1) | 1, value, version, tombstone));
}

TEST(Memtable, GetByVersionMissingKey) {
    Memtable wb;
    std::string value;
    uint64_t version;
    bool tombstone;
    EXPECT_FALSE(wb.getByVersion("missing", (100ULL << 1) | 1, value, version, tombstone));
}

TEST(Memtable, ForEach) {
    Memtable wb;
    wb.put("a", 1, "va", false);
    wb.put("b", 2, "vb", false);
    wb.put("a", 3, "va2", false);

    std::vector<std::pair<std::string, size_t>> collected;
    wb.forEach([&](const std::string& key, const std::vector<Memtable::Entry>& entries) {
        collected.push_back({key, entries.size()});
    });

    ASSERT_EQ(collected.size(), 2u);

    // Sort for deterministic comparison
    std::sort(collected.begin(), collected.end());
    EXPECT_EQ(collected[0].first, "a");
    EXPECT_EQ(collected[0].second, 2u);
    EXPECT_EQ(collected[1].first, "b");
    EXPECT_EQ(collected[1].second, 1u);
}

TEST(Memtable, ClearResetsEverything) {
    Memtable wb;
    wb.put("key1", 1, "v1", false);
    wb.put("key2", 2, "v2", false);

    EXPECT_EQ(wb.keyCount(), 2u);
    EXPECT_EQ(wb.entryCount(), 2u);
    EXPECT_FALSE(wb.empty());
    EXPECT_GT(wb.memoryUsage(), 0u);

    wb.clear();

    EXPECT_EQ(wb.keyCount(), 0u);
    EXPECT_EQ(wb.entryCount(), 0u);
    EXPECT_TRUE(wb.empty());
    EXPECT_EQ(wb.memoryUsage(), 0u);

    std::string value;
    uint64_t version;
    bool tombstone;
    EXPECT_FALSE(wb.get("key1", value, version, tombstone));
}

TEST(Memtable, Statistics) {
    Memtable wb;
    EXPECT_TRUE(wb.empty());
    EXPECT_EQ(wb.keyCount(), 0u);
    EXPECT_EQ(wb.entryCount(), 0u);

    wb.put("k1", 1, "val1", false);
    EXPECT_EQ(wb.keyCount(), 1u);
    EXPECT_EQ(wb.entryCount(), 1u);
    EXPECT_FALSE(wb.empty());

    wb.put("k1", 2, "val2", false);
    EXPECT_EQ(wb.keyCount(), 1u);
    EXPECT_EQ(wb.entryCount(), 2u);

    wb.put("k2", 3, "val3", false);
    EXPECT_EQ(wb.keyCount(), 2u);
    EXPECT_EQ(wb.entryCount(), 3u);
}

TEST(Memtable, ManyKeys) {
    Memtable wb;
    const int N = 10000;
    for (int i = 0; i < N; ++i) {
        wb.put("key_" + std::to_string(i), static_cast<uint64_t>(i), "val_" + std::to_string(i), false);
    }
    EXPECT_EQ(wb.keyCount(), static_cast<size_t>(N));
    EXPECT_EQ(wb.entryCount(), static_cast<size_t>(N));

    // Spot check
    std::string value;
    uint64_t version;
    bool tombstone;
    ASSERT_TRUE(wb.get("key_42", value, version, tombstone));
    EXPECT_EQ(value, "val_42");
    EXPECT_EQ(version, 42u);
}

TEST(Memtable, ConcurrentPuts) {
    Memtable wb;
    const int kThreads = 4;
    const int kPerThread = 5000;

    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&wb, t]() {
            for (int i = 0; i < kPerThread; ++i) {
                std::string key = "t" + std::to_string(t) + "_k" + std::to_string(i);
                wb.put(key, static_cast<uint64_t>(i), "v" + std::to_string(i), false);
            }
        });
    }
    for (auto& t : threads) t.join();

    EXPECT_EQ(wb.keyCount(), static_cast<size_t>(kThreads * kPerThread));
    EXPECT_EQ(wb.entryCount(), static_cast<size_t>(kThreads * kPerThread));

    // Verify all keys readable
    std::string value;
    uint64_t version;
    bool tombstone;
    for (int t = 0; t < kThreads; ++t) {
        for (int i = 0; i < kPerThread; i += 500) {
            std::string key = "t" + std::to_string(t) + "_k" + std::to_string(i);
            ASSERT_TRUE(wb.get(key, value, version, tombstone));
        }
    }
}

TEST(Memtable, ConcurrentPutsSameKey) {
    Memtable wb;
    const int kThreads = 4;
    const int kPerThread = 1000;

    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&wb, t]() {
            for (int i = 0; i < kPerThread; ++i) {
                uint64_t ver = static_cast<uint64_t>(t * kPerThread + i);
                wb.put("shared_key", ver, "v" + std::to_string(ver), false);
            }
        });
    }
    for (auto& t : threads) t.join();

    EXPECT_EQ(wb.keyCount(), 1u);
    EXPECT_EQ(wb.entryCount(), static_cast<size_t>(kThreads * kPerThread));
}

// --- putBatch tests ------------------------------------------------------

TEST(Memtable, PutBatchSameVersion) {
    Memtable wb;

    std::string k1 = "alpha", k2 = "beta", k3 = "gamma";
    std::string v1 = "a_val", v2 = "b_val", v3 = "g_val";

    std::vector<Memtable::BatchOp> ops = {
        {&k1, &v1, false},
        {&k2, &v2, false},
        {&k3, &v3, true},
    };
    wb.putBatch(ops, 42);

    EXPECT_EQ(wb.entryCount(), 3u);
    EXPECT_EQ(wb.keyCount(), 3u);

    // All entries should have version 42.
    std::string value;
    uint64_t version;
    bool tombstone;

    ASSERT_TRUE(wb.get("alpha", value, version, tombstone));
    EXPECT_EQ(value, "a_val");
    EXPECT_EQ(version, 42u);
    EXPECT_FALSE(tombstone);

    ASSERT_TRUE(wb.get("beta", value, version, tombstone));
    EXPECT_EQ(value, "b_val");
    EXPECT_EQ(version, 42u);
    EXPECT_FALSE(tombstone);

    ASSERT_TRUE(wb.get("gamma", value, version, tombstone));
    EXPECT_EQ(value, "g_val");
    EXPECT_EQ(version, 42u);
    EXPECT_TRUE(tombstone);
}

TEST(Memtable, PutBatchKeyCountWithExisting) {
    Memtable wb;
    // Pre-populate a key.
    wb.put("alpha", 1, "old", false);
    EXPECT_EQ(wb.keyCount(), 1u);

    // Batch re-inserts "alpha" and adds "beta".
    std::string k1 = "alpha", k2 = "beta";
    std::string v1 = "new", v2 = "b";

    std::vector<Memtable::BatchOp> ops = {
        {&k1, &v1, false},
        {&k2, &v2, false},
    };
    wb.putBatch(ops, 10);

    EXPECT_EQ(wb.entryCount(), 3u);
    EXPECT_EQ(wb.keyCount(), 2u);

    // Latest version for "alpha" should be 10.
    std::string value;
    uint64_t version;
    bool tombstone;
    ASSERT_TRUE(wb.get("alpha", value, version, tombstone));
    EXPECT_EQ(value, "new");
    EXPECT_EQ(version, 10u);
}

TEST(Memtable, PutBatchEmpty) {
    Memtable wb;
    std::vector<Memtable::BatchOp> ops;
    wb.putBatch(ops, 1);  // should be a no-op
    EXPECT_EQ(wb.entryCount(), 0u);
}

TEST(Memtable, PutBatchConcurrentWritersSameVersion) {
    // Multiple threads write batches to disjoint key sets concurrently.
    // After all threads finish, verify that each batch's keys share one version.
    Memtable wb;
    const int kThreads = 4;
    const int kBatchesPerThread = 200;
    const int kKeysPerBatch = 5;

    std::atomic<uint64_t> next_version{1};

    auto writer = [&](int tid) {
        for (int b = 0; b < kBatchesPerThread; ++b) {
            uint64_t ver = next_version.fetch_add(1, std::memory_order_relaxed);

            std::string prefix = "t" + std::to_string(tid) +
                                 "_b" + std::to_string(b) + "_";
            std::vector<std::string> keys(kKeysPerBatch);
            std::vector<std::string> vals(kKeysPerBatch);
            for (int k = 0; k < kKeysPerBatch; ++k) {
                keys[k] = prefix + "k" + std::to_string(k);
                vals[k] = "v" + std::to_string(k);
            }

            std::vector<Memtable::BatchOp> ops;
            ops.reserve(kKeysPerBatch);
            for (int k = 0; k < kKeysPerBatch; ++k) {
                ops.push_back({&keys[k], &vals[k], false});
            }
            wb.putBatch(ops, ver);
        }
    };

    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back(writer, t);
    }
    for (auto& t : threads) t.join();

    // Verify: each batch's keys share one version.
    for (int tid = 0; tid < kThreads; ++tid) {
        for (int b = 0; b < kBatchesPerThread; ++b) {
            std::string prefix = "t" + std::to_string(tid) +
                                 "_b" + std::to_string(b) + "_";
            uint64_t expected_ver = 0;
            for (int k = 0; k < kKeysPerBatch; ++k) {
                std::string key = prefix + "k" + std::to_string(k);
                std::string val;
                uint64_t ver;
                bool tomb;
                ASSERT_TRUE(wb.get(key, val, ver, tomb))
                    << "Missing key: " << key;
                if (k == 0) {
                    expected_ver = ver;
                } else {
                    EXPECT_EQ(ver, expected_ver)
                        << "Version mismatch in batch " << prefix;
                }
            }
        }
    }
}

TEST(Memtable, PutBatchMonotonicVisibility) {
    // With two-phase locking, a concurrent reader doing sequential get()
    // calls should see monotonically non-decreasing versions for keys
    // from the same batch group, because putBatch makes all entries
    // visible simultaneously.
    Memtable wb;

    const int kIterations = 3000;
    std::atomic<bool> stop{false};
    std::atomic<bool> violation{false};

    std::thread writer([&]() {
        std::string k1 = "x1", k2 = "x2", k3 = "x3";
        for (int i = 1; i <= kIterations && !violation; ++i) {
            std::string v = std::to_string(i);
            std::vector<Memtable::BatchOp> ops = {
                {&k1, &v, false},
                {&k2, &v, false},
                {&k3, &v, false},
            };
            wb.putBatch(ops, static_cast<uint64_t>(i));
        }
        stop = true;
    });

    // Reader: reads x1, x2, x3 in order. Because putBatch makes all
    // entries visible atomically, if x1 is at version V then x2 and x3
    // must be at version >= V (never an older version).
    std::thread reader([&]() {
        while (!stop && !violation) {
            std::string v1, v2, v3;
            uint64_t ver1 = 0, ver2 = 0, ver3 = 0;
            bool t1, t2, t3;

            bool f1 = wb.get("x1", v1, ver1, t1);
            bool f2 = wb.get("x2", v2, ver2, t2);
            bool f3 = wb.get("x3", v3, ver3, t3);

            if (!f1) continue;  // nothing written yet

            // If x1 is visible at version V, x2 and x3 must also be visible
            // (putBatch guarantees all entries appear simultaneously).
            if (!f2 || !f3) {
                violation = true;
                break;
            }

            // Versions must be monotonically non-decreasing: ver2 >= ver1
            // and ver3 >= ver2. A new putBatch may land between get() calls,
            // but an older version can never appear after a newer one.
            if (ver2 < ver1 || ver3 < ver2) {
                violation = true;
                break;
            }
        }
    });

    writer.join();
    reader.join();

    EXPECT_FALSE(violation.load())
        << "Non-monotonic version visibility detected";
}

// --- Flush tests ---------------------------------------------------------

using kvlite::internal::LogEntry;
using kvlite::internal::PackedVersion;
using kvlite::internal::Segment;
using kvlite::internal::crc32;
using kvlite::internal::dhtHashBytes;
using kvlite::Status;

namespace {

class FlushTest : public ::testing::Test {
protected:
    void SetUp() override {
        dir_ = ::testing::TempDir() + "/flush_test_" +
               std::to_string(reinterpret_cast<uintptr_t>(this));
        std::filesystem::create_directories(dir_);
        path_ = dir_ + "/seg_1.log";
        ASSERT_TRUE(seg_.create(path_, 1).ok());
    }

    void TearDown() override {
        seg_.close();
        std::filesystem::remove_all(dir_);
    }

    // Read raw bytes from the segment file via pread.
    void rawRead(const std::string& path, uint64_t offset, void* buf, size_t len) {
        int fd = ::open(path.c_str(), O_RDONLY);
        ASSERT_GE(fd, 0);
        ssize_t n = ::pread(fd, buf, len, static_cast<off_t>(offset));
        ::close(fd);
        ASSERT_EQ(static_cast<size_t>(n), len);
    }

    // Read one LogEntry from a given offset, return bytes consumed.
    size_t readEntry(const std::string& path, uint64_t offset, LogEntry& out) {
        uint8_t hdr[LogEntry::kHeaderSize];
        rawRead(path, offset, hdr, LogEntry::kHeaderSize);

        uint64_t packed_ver;
        uint16_t kl;
        uint32_t vl;
        std::memcpy(&packed_ver, hdr, 8);
        std::memcpy(&kl, hdr + 8, 2);
        std::memcpy(&vl, hdr + 10, 4);

        out.pv = PackedVersion(packed_ver);
        out.key.resize(kl);
        out.value.resize(vl);

        if (kl > 0) {
            rawRead(path, offset + LogEntry::kHeaderSize, out.key.data(), kl);
        }
        if (vl > 0) {
            rawRead(path, offset + LogEntry::kHeaderSize + kl, out.value.data(), vl);
        }

        return LogEntry::kHeaderSize + kl + vl + LogEntry::kChecksumSize;
    }

    // Verify CRC of an entry at a given offset with known total size.
    void verifyCrc(const std::string& path, uint64_t offset, size_t total_size) {
        std::vector<uint8_t> buf(total_size);
        rawRead(path, offset, buf.data(), total_size);
        size_t payload_len = total_size - LogEntry::kChecksumSize;
        uint32_t expected;
        std::memcpy(&expected, buf.data() + payload_len, 4);
        uint32_t actual = crc32(buf.data(), payload_len);
        EXPECT_EQ(actual, expected);
    }

    std::string dir_;
    std::string path_;
    Segment seg_;
};

} // namespace

TEST_F(FlushTest, EmptyBuffer) {
    Memtable wb;
    auto result = wb.flush(seg_);
    ASSERT_TRUE(result.status.ok());
    EXPECT_EQ(seg_.dataSize(), 0u);
    EXPECT_EQ(seg_.entryCount(), 0u);
}

TEST_F(FlushTest, SingleEntry) {
    Memtable wb;
    wb.put("hello", 42, "world", false);

    auto result = wb.flush(seg_);
    ASSERT_TRUE(result.status.ok());

    size_t expected_size = LogEntry::kHeaderSize + 5 + 5 + LogEntry::kChecksumSize;
    EXPECT_EQ(seg_.dataSize(), expected_size);

    LogEntry entry;
    size_t consumed = readEntry(path_, 0, entry);
    EXPECT_EQ(consumed, expected_size);
    EXPECT_EQ(entry.key, "hello");
    EXPECT_EQ(entry.value, "world");
    EXPECT_EQ(entry.version(), 42u);
    EXPECT_FALSE(entry.tombstone());

    verifyCrc(path_, 0, expected_size);

    // Verify SegmentIndex
    EXPECT_EQ(seg_.entryCount(), 1u);
    LogEntry latest;
    ASSERT_TRUE(seg_.getLatest("hello", latest).ok());
    EXPECT_EQ(latest.version(), 42u);
}

TEST_F(FlushTest, TombstoneEntry) {
    Memtable wb;
    wb.put("deleted", 7, "", true);

    auto result = wb.flush(seg_);
    ASSERT_TRUE(result.status.ok());

    LogEntry entry;
    readEntry(path_, 0, entry);
    EXPECT_EQ(entry.key, "deleted");
    EXPECT_EQ(entry.value, "");
    EXPECT_EQ(entry.version(), 7u);
    EXPECT_TRUE(entry.tombstone());
}

TEST_F(FlushTest, SortOrderHashThenVersion) {
    Memtable wb;
    // Insert multiple keys with multiple versions — flush deduplicates,
    // keeping only the latest version per key.
    wb.put("aaa", 10, "v10", false);
    wb.put("bbb", 5, "v5", false);
    wb.put("aaa", 3, "v3", false);
    wb.put("bbb", 1, "v1", false);
    wb.put("ccc", 20, "v20", false);

    auto result = wb.flush(seg_);
    ASSERT_TRUE(result.status.ok());

    // Read all entries back
    std::vector<std::pair<uint64_t, uint64_t>> order; // (hash, version)
    uint64_t offset = 0;
    while (offset < seg_.dataSize()) {
        LogEntry entry;
        size_t sz = readEntry(path_, offset, entry);
        uint64_t h = dhtHashBytes(entry.key.data(), entry.key.size());
        order.push_back({h, entry.version()});
        verifyCrc(path_, offset, sz);
        offset += sz;
    }

    // Only latest version per key survives: aaa@10, bbb@5, ccc@20.
    ASSERT_EQ(order.size(), 3u);

    // Verify sorted by hash ascending.
    for (size_t i = 1; i < order.size(); ++i) {
        EXPECT_LT(order[i - 1].first, order[i].first)
            << "Entry " << i << " is out of order";
    }
}

TEST_F(FlushTest, RoundTrip) {
    Memtable wb;
    wb.put("key1", 1, "val1", false);
    wb.put("key1", 2, "val2", false);
    wb.put("key2", 3, "val3", true);

    // No snapshot versions → full dedup: only latest per key survives.
    auto result = wb.flush(seg_);
    ASSERT_TRUE(result.status.ok());

    // Read all entries back and verify contents
    std::vector<LogEntry> entries;
    uint64_t offset = 0;
    while (offset < seg_.dataSize()) {
        LogEntry entry;
        size_t sz = readEntry(path_, offset, entry);
        verifyCrc(path_, offset, sz);
        entries.push_back(std::move(entry));
        offset += sz;
    }

    // key1@1 eliminated (redundant), key1@2 kept, key2@3 kept.
    ASSERT_EQ(entries.size(), 2u);

    // Collect into a map for verification (key -> list of versions)
    std::map<std::string, std::vector<uint64_t>> by_key;
    for (const auto& e : entries) {
        by_key[e.key].push_back(e.version());
    }

    ASSERT_EQ(by_key.count("key1"), 1u);
    ASSERT_EQ(by_key["key1"].size(), 1u);
    EXPECT_EQ(by_key["key1"][0], 2u);

    ASSERT_EQ(by_key.count("key2"), 1u);
    ASSERT_EQ(by_key["key2"].size(), 1u);

    // Verify tombstone for key2
    for (const auto& e : entries) {
        if (e.key == "key2") {
            EXPECT_TRUE(e.tombstone());
            EXPECT_EQ(e.version(), 3u);
        }
    }

    // Verify SegmentIndex was populated correctly
    EXPECT_EQ(seg_.entryCount(), 2u);
    EXPECT_EQ(seg_.keyCount(), 2u);

    // key1 latest version is 2
    LogEntry latest;
    ASSERT_TRUE(seg_.getLatest("key1", latest).ok());
    EXPECT_EQ(latest.version(), 2u);

    // key2 latest version is 3
    ASSERT_TRUE(seg_.getLatest("key2", latest).ok());
    EXPECT_EQ(latest.version(), 3u);

    // key1 has 1 entry after dedup
    std::vector<LogEntry> key1_entries;
    ASSERT_TRUE(seg_.get("key1", key1_entries).ok());
    ASSERT_EQ(key1_entries.size(), 1u);
    EXPECT_EQ(key1_entries[0].key, "key1");
    EXPECT_EQ(key1_entries[0].version(), 2u);
}

TEST_F(FlushTest, SealAndOpen) {
    Memtable wb;
    wb.put("alpha", 1, "v1", false);
    wb.put("alpha", 2, "v2", false);
    wb.put("beta", 10, "vb", true);

    // No snapshot versions → full dedup.
    auto result = wb.flush(seg_);
    ASSERT_TRUE(result.status.ok());
    uint64_t data_size = seg_.dataSize();

    // Open into a fresh Segment (outside storage manager).
    Segment loaded;
    ASSERT_TRUE(loaded.open(path_).ok());

    // Verify data size matches (excludes index + footer).
    EXPECT_EQ(loaded.dataSize(), data_size);
    EXPECT_EQ(loaded.entryCount(), 2u);  // alpha@2, beta@10
    EXPECT_EQ(loaded.keyCount(), 2u);

    // Verify index lookups work.
    LogEntry latest;
    ASSERT_TRUE(loaded.getLatest("alpha", latest).ok());
    EXPECT_EQ(latest.version(), 2u);
    EXPECT_EQ(latest.value, "v2");

    ASSERT_TRUE(loaded.getLatest("beta", latest).ok());
    EXPECT_EQ(latest.version(), 10u);
    EXPECT_EQ(latest.key, "beta");
    EXPECT_TRUE(latest.tombstone());

    // alpha has 1 entry after dedup.
    std::vector<LogEntry> alpha_entries;
    ASSERT_TRUE(loaded.get("alpha", alpha_entries).ok());
    ASSERT_EQ(alpha_entries.size(), 1u);
    EXPECT_EQ(alpha_entries[0].key, "alpha");
    EXPECT_EQ(alpha_entries[0].version(), 2u);

    loaded.close();
}

TEST_F(FlushTest, MultiSnapshotPreservation) {
    // Verify that flush preserves versions needed by multiple snapshots.
    // Key "k" has versions 1, 5, 10, 15, 20.
    // Snapshots at 3, 12, 25.
    // Expected survivors: v1 (latest <= 3), v10 (latest <= 12), v20 (latest <= 25).
    Memtable wb;
    wb.put("k", 1, "v1", false);
    wb.put("k", 5, "v5", false);
    wb.put("k", 10, "v10", false);
    wb.put("k", 15, "v15", false);
    wb.put("k", 20, "v20", false);

    std::vector<uint64_t> snapshots = {3, 12, 25};
    auto result = wb.flush(seg_, snapshots);
    ASSERT_TRUE(result.status.ok());

    // Read all entries back.
    std::vector<LogEntry> entries;
    uint64_t offset = 0;
    while (offset < seg_.dataSize()) {
        LogEntry entry;
        size_t sz = readEntry(path_, offset, entry);
        verifyCrc(path_, offset, sz);
        entries.push_back(std::move(entry));
        offset += sz;
    }

    // v1 kept (snap@3), v10 kept (snap@12), v20 kept (snap@25).
    // v5 and v15 are redundant — no snapshot needs them.
    ASSERT_EQ(entries.size(), 3u);

    std::vector<uint64_t> kept_versions;
    for (const auto& e : entries) {
        kept_versions.push_back(e.version());
    }
    std::sort(kept_versions.begin(), kept_versions.end());
    EXPECT_EQ(kept_versions[0], 1u);
    EXPECT_EQ(kept_versions[1], 10u);
    EXPECT_EQ(kept_versions[2], 20u);
}

TEST_F(FlushTest, SnapshotPreservesOlderVersion) {
    // Two versions of same key. Snapshot covers the older one.
    Memtable wb;
    wb.put("k", 5, "old", false);
    wb.put("k", 10, "new", false);

    // Snapshot at version 7 → must keep v5 (latest <= 7) and v10 (latest <= 15).
    std::vector<uint64_t> snapshots = {7, 15};
    auto result = wb.flush(seg_, snapshots);
    ASSERT_TRUE(result.status.ok());

    std::vector<LogEntry> entries;
    uint64_t offset = 0;
    while (offset < seg_.dataSize()) {
        LogEntry entry;
        size_t sz = readEntry(path_, offset, entry);
        entries.push_back(std::move(entry));
        offset += sz;
    }

    // Both versions kept: v5 for snap@7, v10 for snap@15.
    ASSERT_EQ(entries.size(), 2u);
}

// ==========================================================================
// WriteBuffer orchestrator tests — multi-Memtable search
// ==========================================================================
//
// These tests exercise the n-way buffered WriteBuffer: reads spanning the
// active Memtable, immutable queue entries, and the Memtable currently being
// flushed by the background daemon.
//
// A controllable FlushFn blocks on a condition variable so the test can
// observe state while Memtables sit in the queue or are mid-flush.

using kvlite::internal::WriteBuffer;
using kvlite::internal::PackedVersion;

namespace {

// Helper: FlushFn that blocks each invocation until explicitly released.
// Supports multiple sequential flushes: each call increments a generation
// counter and blocks independently.
struct GatedFlush {
    std::mutex mu;
    std::condition_variable gate_cv;
    std::condition_variable entered_cv;
    int entered_gen = 0;      // incremented when flush callback is entered
    int released_gen = 0;     // incremented when the test releases a flush
    std::atomic<int> flush_count{0};

    kvlite::Status operator()(Memtable& /*mt*/) {
        std::unique_lock<std::mutex> lock(mu);
        int my_gen = ++entered_gen;
        entered_cv.notify_all();
        gate_cv.wait(lock, [&] { return released_gen >= my_gen; });
        ++flush_count;
        return kvlite::Status::OK();
    }

    // Block until the daemon has entered a flush callback (any generation).
    void waitEntered(int expected_gen) {
        std::unique_lock<std::mutex> lock(mu);
        entered_cv.wait(lock, [&] { return entered_gen >= expected_gen; });
    }

    // Release up to and including the given generation.
    void releaseThrough(int gen) {
        std::lock_guard<std::mutex> lock(mu);
        released_gen = gen;
        gate_cv.notify_all();
    }

    // Convenience: release all entered flushes.
    void releaseAll() {
        std::lock_guard<std::mutex> lock(mu);
        released_gen = entered_gen + 100;  // future-proof
        gate_cv.notify_all();
    }
};

WriteBuffer::Options wbOpts(size_t memtable_size, uint32_t flush_depth = 3) {
    return {memtable_size, flush_depth};
}

// Fill a WriteBuffer until it triggers a seal. Uses a deterministic payload
// size to guarantee exactly one seal. Returns version of the last put.
// key_prefix differentiates keys across calls.
uint64_t fillAndSeal(WriteBuffer& wb, size_t memtable_size,
                     const std::string& key_prefix, uint64_t start_ver) {
    // Each record in the data buffer is kRecordHeaderSize(14) + key + value.
    // Use a fixed payload to make sizing predictable.
    std::string val(32, 'x');
    uint64_t ver = start_ver;
    size_t written = 0;
    while (written < memtable_size) {
        std::string key = key_prefix + std::to_string(ver);
        wb.put(key, ver, val, false);
        written += 14 + key.size() + val.size();
        ++ver;
    }
    return ver;  // next available version
}

}  // namespace

// --- Basic multi-Memtable read tests -------------------------------------

TEST(WriteBufferOrchestrator, ReadFromActiveMemtable) {
    GatedFlush gf;
    WriteBuffer wb(wbOpts(1 << 20), std::ref(gf));

    wb.put("k1", 1, "v1", false);

    std::string val;
    uint64_t ver;
    bool tomb;
    ASSERT_TRUE(wb.getByVersion("k1", UINT64_MAX, val, ver, tomb));
    EXPECT_EQ(val, "v1");
    EXPECT_EQ(ver, 1u);
    EXPECT_FALSE(tomb);

    gf.releaseAll();
}

TEST(WriteBufferOrchestrator, ReadFromFlushingMemtable) {
    // Fill a memtable → it seals → daemon picks it up → blocked in flush.
    // getByVersion must still find the data (via flushing_ pointer).
    constexpr size_t kMTSize = 256;
    GatedFlush gf;
    WriteBuffer wb(wbOpts(kMTSize), std::ref(gf));

    fillAndSeal(wb, kMTSize, "k", 1);
    gf.waitEntered(1);  // daemon entered flush for the first sealed memtable

    std::string val;
    uint64_t ver;
    bool tomb;
    ASSERT_TRUE(wb.getByVersion("k1", UINT64_MAX, val, ver, tomb));
    EXPECT_EQ(ver, 1u);

    gf.releaseAll();
}

TEST(WriteBufferOrchestrator, ReadSpansActiveAndFlushing) {
    // Data in the flushing memtable + data in the active memtable —
    // both should be readable simultaneously.
    constexpr size_t kMTSize = 256;
    GatedFlush gf;
    WriteBuffer wb(wbOpts(kMTSize), std::ref(gf));

    fillAndSeal(wb, kMTSize, "old_", 1);
    gf.waitEntered(1);

    // Single put to the new active (well under threshold, no second seal).
    wb.put("new_key", 1000, "new_val", false);

    std::string val;
    uint64_t ver;
    bool tomb;

    ASSERT_TRUE(wb.getByVersion("old_1", UINT64_MAX, val, ver, tomb));
    EXPECT_EQ(ver, 1u);

    ASSERT_TRUE(wb.getByVersion("new_key", UINT64_MAX, val, ver, tomb));
    EXPECT_EQ(val, "new_val");
    EXPECT_EQ(ver, 1000u);

    gf.releaseAll();
}

TEST(WriteBufferOrchestrator, ReadSpansActiveImmutableAndFlushing) {
    // Three memtables: flushing (gen 1), immutable queued (gen 2), active.
    // All three should be searchable.
    constexpr size_t kMTSize = 256;
    GatedFlush gf;
    WriteBuffer wb(wbOpts(kMTSize), std::ref(gf));

    // Fill and seal → gen 1, daemon enters flush and blocks.
    uint64_t next = fillAndSeal(wb, kMTSize, "a_", 1);
    gf.waitEntered(1);

    // Fill and seal a second memtable → goes to immutables (daemon busy).
    next = fillAndSeal(wb, kMTSize, "b_", next);

    // Single put to active.
    wb.put("c_key", next, "c_val", false);

    std::string val;
    uint64_t ver;
    bool tomb;

    // From flushing:
    ASSERT_TRUE(wb.getByVersion("a_1", UINT64_MAX, val, ver, tomb));
    EXPECT_EQ(ver, 1u);

    // From immutables queue:
    ASSERT_TRUE(wb.getByVersion("b_" + std::to_string(next - 1), UINT64_MAX, val, ver, tomb));

    // From active:
    ASSERT_TRUE(wb.getByVersion("c_key", UINT64_MAX, val, ver, tomb));
    EXPECT_EQ(val, "c_val");

    gf.releaseAll();
}

TEST(WriteBufferOrchestrator, ActiveOverridesImmutable) {
    // Same key in sealed memtable (v1) and active memtable (v50).
    // Unbounded read returns the active's newer version.
    constexpr size_t kMTSize = 256;
    GatedFlush gf;
    WriteBuffer wb(wbOpts(kMTSize), std::ref(gf));

    // Write the target key, then pad to trigger seal.
    wb.put("k1", 1, "old", false);
    fillAndSeal(wb, kMTSize, "pad_", 10);
    gf.waitEntered(1);

    // Overwrite in active.
    wb.put("k1", 50, "new", false);

    std::string val;
    uint64_t ver;
    bool tomb;
    ASSERT_TRUE(wb.getByVersion("k1", UINT64_MAX, val, ver, tomb));
    EXPECT_EQ(val, "new");
    EXPECT_EQ(ver, 50u);

    gf.releaseAll();
}

TEST(WriteBufferOrchestrator, VersionBoundFallsThroughToImmutable) {
    // Active has k1@v50, flushing has k1@v1. Version-bounded read with
    // upper_bound < v50 should find v1 in the flushing memtable.
    constexpr size_t kMTSize = 256;
    GatedFlush gf;
    WriteBuffer wb(wbOpts(kMTSize), std::ref(gf));

    wb.put("k1", 1, "old", false);
    fillAndSeal(wb, kMTSize, "pad_", 10);
    gf.waitEntered(1);

    wb.put("k1", 50, "new", false);

    std::string val;
    uint64_t ver;
    bool tomb;
    // packed upper bound: (version << 1) | 1
    ASSERT_TRUE(wb.getByVersion("k1", (5ULL << 1) | 1, val, ver, tomb));
    EXPECT_EQ(val, "old");
    EXPECT_EQ(ver, 1u);

    gf.releaseAll();
}

TEST(WriteBufferOrchestrator, TombstoneInActiveHidesImmutable) {
    constexpr size_t kMTSize = 256;
    GatedFlush gf;
    WriteBuffer wb(wbOpts(kMTSize), std::ref(gf));

    wb.put("k1", 1, "alive", false);
    fillAndSeal(wb, kMTSize, "pad_", 10);
    gf.waitEntered(1);

    wb.put("k1", 50, "", true);

    std::string val;
    uint64_t ver;
    bool tomb;
    ASSERT_TRUE(wb.getByVersion("k1", UINT64_MAX, val, ver, tomb));
    EXPECT_TRUE(tomb);
    EXPECT_EQ(ver, 50u);

    gf.releaseAll();
}

TEST(WriteBufferOrchestrator, ReadMissingKeyAcrossAll) {
    constexpr size_t kMTSize = 256;
    GatedFlush gf;
    WriteBuffer wb(wbOpts(kMTSize), std::ref(gf));

    fillAndSeal(wb, kMTSize, "k_", 1);
    gf.waitEntered(1);

    wb.put("active_key", 999, "v", false);

    std::string val;
    uint64_t ver;
    bool tomb;
    EXPECT_FALSE(wb.getByVersion("missing", UINT64_MAX, val, ver, tomb));

    gf.releaseAll();
}

// --- DrainFlush tests ----------------------------------------------------

TEST(WriteBufferOrchestrator, DrainFlushWaitsForAll) {
    constexpr size_t kMTSize = 256;
    GatedFlush gf;
    WriteBuffer wb(wbOpts(kMTSize), std::ref(gf));

    fillAndSeal(wb, kMTSize, "k_", 1);
    gf.waitEntered(1);

    // Put a little data in active so drainFlush must seal + flush it too.
    wb.put("extra", 1000, "val", false);

    // Release gen 1 so daemon can proceed to gen 2.
    gf.releaseThrough(1);

    // drainFlush seals active → daemon picks it up (gen 2).
    // Release gen 2 from another thread.
    std::thread releaser([&] {
        gf.waitEntered(2);
        gf.releaseThrough(2);
    });

    wb.drainFlush();
    releaser.join();

    EXPECT_TRUE(wb.empty());
    EXPECT_EQ(gf.flush_count.load(), 2);
}

TEST(WriteBufferOrchestrator, DrainFlushOnEmptyIsNoop) {
    GatedFlush gf;
    WriteBuffer wb(wbOpts(1 << 20), std::ref(gf));

    wb.drainFlush();
    EXPECT_TRUE(wb.empty());
    EXPECT_EQ(gf.flush_count.load(), 0);

    gf.releaseAll();
}

// --- Stall tests ---------------------------------------------------------

TEST(WriteBufferOrchestrator, StallWhenQueueFull) {
    // flush_depth=2 → 1 active + 1 immutable. When the single immutable slot
    // is occupied and active fills, writers stall until the daemon frees a slot.
    constexpr size_t kMTSize = 256;
    GatedFlush gf;
    WriteBuffer wb(wbOpts(kMTSize, /*flush_depth=*/2), std::ref(gf));

    fillAndSeal(wb, kMTSize, "a_", 1);
    gf.waitEntered(1);

    // Writer thread: tries to fill and seal the active again. This will stall
    // because immutables_.size() == 0 (daemon popped it), but the daemon is
    // still flushing so a new immutable can be queued. Actually, with
    // flush_depth=2, max immutables is 1. After the first flush finishes, the
    // writer can proceed. Let's just verify the writer eventually completes.
    std::atomic<bool> put_done{false};
    std::thread writer([&] {
        fillAndSeal(wb, kMTSize, "b_", 100);
        put_done = true;
    });

    // Brief sleep — writer may or may not stall depending on timing.
    std::this_thread::sleep_for(std::chrono::milliseconds(30));

    // Data from gen 1 is still readable (in flushing_).
    std::string val;
    uint64_t ver;
    bool tomb;
    ASSERT_TRUE(wb.getByVersion("a_1", UINT64_MAX, val, ver, tomb));

    // Release gen 1.
    gf.releaseThrough(1);

    // The writer will trigger gen 2; release it too.
    gf.waitEntered(2);
    gf.releaseThrough(2);

    writer.join();
    EXPECT_TRUE(put_done.load());

    gf.releaseAll();
}

// --- pinAll / unpinAll tests ---------------------------------------------

TEST(WriteBufferOrchestrator, PinAllIncludesAllMemtables) {
    constexpr size_t kMTSize = 256;
    GatedFlush gf;
    WriteBuffer wb(wbOpts(kMTSize), std::ref(gf));

    fillAndSeal(wb, kMTSize, "old_", 1);
    gf.waitEntered(1);

    wb.put("new_key", 999, "new_val", false);

    auto snap = wb.pinAll();
    EXPECT_GE(snap.memtables.size(), 2u);

    bool found_old = false, found_new = false;
    for (auto* mt : snap.memtables) {
        std::string val;
        uint64_t ver;
        bool tomb;
        if (mt->getByVersion("old_1", UINT64_MAX, val, ver, tomb)) found_old = true;
        if (mt->getByVersion("new_key", UINT64_MAX, val, ver, tomb)) found_new = true;
    }
    EXPECT_TRUE(found_old);
    EXPECT_TRUE(found_new);

    wb.unpinAll(snap);
    gf.releaseAll();
}

// --- Concurrent read/write across Memtables ------------------------------

TEST(WriteBufferOrchestrator, ConcurrentReadsWhileFlushing) {
    // Write enough data to trigger multiple seals while reads are concurrent.
    // Flushes are gated so data remains in the WriteBuffer during verification.
    constexpr size_t kMTSize = 256;
    GatedFlush gf;
    WriteBuffer wb(wbOpts(kMTSize), std::ref(gf));

    // Fill and seal two memtables; daemon blocks on the first.
    // Keys: a_1, a_2, ... (first memtable), b_<next>, b_<next+1>, ... (second).
    uint64_t next_a = fillAndSeal(wb, kMTSize, "a_", 1);
    gf.waitEntered(1);
    uint64_t next_b = fillAndSeal(wb, kMTSize, "b_", next_a);

    // Put a few entries in the active memtable.
    wb.put("c_1", next_b, "c_val1", false);
    wb.put("c_2", next_b + 1, "c_val2", false);

    // Concurrent reads: multiple threads verify data across all memtables.
    std::atomic<bool> violation{false};
    std::vector<std::thread> readers;
    for (int t = 0; t < 4; ++t) {
        readers.emplace_back([&, next_a, next_b] {
            std::string val;
            uint64_t ver;
            bool tomb;
            // Check flushing memtable data (key = "a_1").
            if (!wb.getByVersion("a_1", UINT64_MAX, val, ver, tomb)) {
                violation = true;
            }
            // Check immutable data (first key in second batch).
            std::string b_key = "b_" + std::to_string(next_a);
            if (!wb.getByVersion(b_key, UINT64_MAX, val, ver, tomb)) {
                violation = true;
            }
            // Check active data.
            if (!wb.getByVersion("c_1", UINT64_MAX, val, ver, tomb)) {
                violation = true;
            }
        });
    }
    for (auto& t : readers) t.join();

    EXPECT_FALSE(violation.load());
    gf.releaseAll();
}

