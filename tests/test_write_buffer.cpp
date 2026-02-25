#include <gtest/gtest.h>

#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include "internal/crc32.h"
#include "internal/delta_hash_table.h"
#include "internal/global_index.h"
#include "internal/segment.h"
#include "internal/write_buffer.h"

using kvlite::internal::WriteBuffer;

TEST(WriteBuffer, PutAndGetSingleEntry) {
    WriteBuffer wb;
    wb.put("key1", 1, "value1", false);

    std::string value;
    uint64_t version;
    bool tombstone;
    ASSERT_TRUE(wb.get("key1", value, version, tombstone));
    EXPECT_EQ(value, "value1");
    EXPECT_EQ(version, 1u);
    EXPECT_FALSE(tombstone);
}

TEST(WriteBuffer, GetMissing) {
    WriteBuffer wb;
    std::string value;
    uint64_t version;
    bool tombstone;
    EXPECT_FALSE(wb.get("missing", value, version, tombstone));
}

TEST(WriteBuffer, GetReturnsLatestVersion) {
    WriteBuffer wb;
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

TEST(WriteBuffer, Tombstone) {
    WriteBuffer wb;
    wb.put("key1", 1, "", true);

    std::string value;
    uint64_t version;
    bool tombstone;
    ASSERT_TRUE(wb.get("key1", value, version, tombstone));
    EXPECT_TRUE(tombstone);
    EXPECT_EQ(version, 1u);
}

TEST(WriteBuffer, GetByVersionExact) {
    WriteBuffer wb;
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

TEST(WriteBuffer, GetByVersionBetween) {
    WriteBuffer wb;
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

TEST(WriteBuffer, GetByVersionTooLow) {
    WriteBuffer wb;
    wb.put("key1", 5, "v5", false);

    std::string value;
    uint64_t version;
    bool tombstone;
    EXPECT_FALSE(wb.getByVersion("key1", (3ULL << 1) | 1, value, version, tombstone));
}

TEST(WriteBuffer, GetByVersionMissingKey) {
    WriteBuffer wb;
    std::string value;
    uint64_t version;
    bool tombstone;
    EXPECT_FALSE(wb.getByVersion("missing", (100ULL << 1) | 1, value, version, tombstone));
}

TEST(WriteBuffer, ForEach) {
    WriteBuffer wb;
    wb.put("a", 1, "va", false);
    wb.put("b", 2, "vb", false);
    wb.put("a", 3, "va2", false);

    std::vector<std::pair<std::string, size_t>> collected;
    wb.forEach([&](const std::string& key, const std::vector<WriteBuffer::Entry>& entries) {
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

TEST(WriteBuffer, ClearResetsEverything) {
    WriteBuffer wb;
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

TEST(WriteBuffer, Statistics) {
    WriteBuffer wb;
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

TEST(WriteBuffer, ManyKeys) {
    WriteBuffer wb;
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

TEST(WriteBuffer, ConcurrentPuts) {
    WriteBuffer wb;
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

TEST(WriteBuffer, ConcurrentPutsSameKey) {
    WriteBuffer wb;
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

TEST(WriteBuffer, PutBatchSameVersion) {
    WriteBuffer wb;

    std::string k1 = "alpha", k2 = "beta", k3 = "gamma";
    std::string v1 = "a_val", v2 = "b_val", v3 = "g_val";

    std::vector<WriteBuffer::BatchOp> ops = {
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

TEST(WriteBuffer, PutBatchKeyCountWithExisting) {
    WriteBuffer wb;
    // Pre-populate a key.
    wb.put("alpha", 1, "old", false);
    EXPECT_EQ(wb.keyCount(), 1u);

    // Batch re-inserts "alpha" and adds "beta".
    std::string k1 = "alpha", k2 = "beta";
    std::string v1 = "new", v2 = "b";

    std::vector<WriteBuffer::BatchOp> ops = {
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

TEST(WriteBuffer, PutBatchEmpty) {
    WriteBuffer wb;
    std::vector<WriteBuffer::BatchOp> ops;
    wb.putBatch(ops, 1);  // should be a no-op
    EXPECT_EQ(wb.entryCount(), 0u);
}

TEST(WriteBuffer, PutBatchConcurrentWritersSameVersion) {
    // Multiple threads write batches to disjoint key sets concurrently.
    // After all threads finish, verify that each batch's keys share one version.
    WriteBuffer wb;
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

            std::vector<WriteBuffer::BatchOp> ops;
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

TEST(WriteBuffer, PutBatchMonotonicVisibility) {
    // With two-phase locking, a concurrent reader doing sequential get()
    // calls should see monotonically non-decreasing versions for keys
    // from the same batch group, because putBatch makes all entries
    // visible simultaneously.
    WriteBuffer wb;

    const int kIterations = 3000;
    std::atomic<bool> stop{false};
    std::atomic<bool> violation{false};

    std::thread writer([&]() {
        std::string k1 = "x1", k2 = "x2", k3 = "x3";
        for (int i = 1; i <= kIterations && !violation; ++i) {
            std::string v = std::to_string(i);
            std::vector<WriteBuffer::BatchOp> ops = {
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

using kvlite::internal::GlobalIndex;
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
        path_ = ::testing::TempDir() + "/flush_test_" +
                std::to_string(reinterpret_cast<uintptr_t>(this)) + ".data";
    }

    void TearDown() override {
        if (seg_.isOpen()) seg_.close();
        std::remove(path_.c_str());
    }

    // Read raw bytes from the segment file via pread.
    void rawRead(uint64_t offset, void* buf, size_t len) {
        int fd = ::open(path_.c_str(), O_RDONLY);
        ASSERT_GE(fd, 0);
        ssize_t n = ::pread(fd, buf, len, static_cast<off_t>(offset));
        ::close(fd);
        ASSERT_EQ(static_cast<size_t>(n), len);
    }

    // Read one LogEntry from a given offset, return bytes consumed.
    size_t readEntry(uint64_t offset, LogEntry& out) {
        uint8_t hdr[LogEntry::kHeaderSize];
        rawRead(offset, hdr, LogEntry::kHeaderSize);

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
            rawRead(offset + LogEntry::kHeaderSize, out.key.data(), kl);
        }
        if (vl > 0) {
            rawRead(offset + LogEntry::kHeaderSize + kl, out.value.data(), vl);
        }

        return LogEntry::kHeaderSize + kl + vl + LogEntry::kChecksumSize;
    }

    // Verify CRC of an entry at a given offset with known total size.
    void verifyCrc(uint64_t offset, size_t total_size) {
        std::vector<uint8_t> buf(total_size);
        rawRead(offset, buf.data(), total_size);
        size_t payload_len = total_size - LogEntry::kChecksumSize;
        uint32_t expected;
        std::memcpy(&expected, buf.data() + payload_len, 4);
        uint32_t actual = crc32(buf.data(), payload_len);
        EXPECT_EQ(actual, expected);
    }

    std::string path_;
    Segment seg_;
    GlobalIndex global_index_;
};

} // namespace

TEST_F(FlushTest, EmptyBuffer) {
    WriteBuffer wb;
    ASSERT_TRUE(seg_.create(path_, 1).ok());
    ASSERT_TRUE(wb.flush(seg_, 1, global_index_).ok());
    EXPECT_EQ(seg_.dataSize(), 0u);
    EXPECT_EQ(seg_.entryCount(), 0u);
}

TEST_F(FlushTest, SingleEntry) {
    WriteBuffer wb;
    wb.put("hello", 42, "world", false);

    ASSERT_TRUE(seg_.create(path_, 1).ok());
    ASSERT_TRUE(wb.flush(seg_, 1, global_index_).ok());

    size_t expected_size = LogEntry::kHeaderSize + 5 + 5 + LogEntry::kChecksumSize;
    EXPECT_EQ(seg_.dataSize(), expected_size);

    LogEntry entry;
    size_t consumed = readEntry(0, entry);
    EXPECT_EQ(consumed, expected_size);
    EXPECT_EQ(entry.key, "hello");
    EXPECT_EQ(entry.value, "world");
    EXPECT_EQ(entry.version(), 42u);
    EXPECT_FALSE(entry.tombstone());

    verifyCrc(0, expected_size);

    // Verify SegmentIndex
    EXPECT_EQ(seg_.entryCount(), 1u);
    LogEntry latest;
    ASSERT_TRUE(seg_.getLatest("hello", latest).ok());
    EXPECT_EQ(latest.version(), 42u);
}

TEST_F(FlushTest, TombstoneEntry) {
    WriteBuffer wb;
    wb.put("deleted", 7, "", true);

    ASSERT_TRUE(seg_.create(path_, 1).ok());
    ASSERT_TRUE(wb.flush(seg_, 1, global_index_).ok());

    LogEntry entry;
    readEntry(0, entry);
    EXPECT_EQ(entry.key, "deleted");
    EXPECT_EQ(entry.value, "");
    EXPECT_EQ(entry.version(), 7u);
    EXPECT_TRUE(entry.tombstone());
}

TEST_F(FlushTest, SortOrderHashThenVersion) {
    WriteBuffer wb;
    // Insert multiple keys with multiple versions
    wb.put("aaa", 10, "v10", false);
    wb.put("bbb", 5, "v5", false);
    wb.put("aaa", 3, "v3", false);
    wb.put("bbb", 1, "v1", false);
    wb.put("ccc", 20, "v20", false);

    ASSERT_TRUE(seg_.create(path_, 1).ok());
    ASSERT_TRUE(wb.flush(seg_, 1, global_index_).ok());

    // Read all entries back
    std::vector<std::pair<uint64_t, uint64_t>> order; // (hash, version)
    uint64_t offset = 0;
    while (offset < seg_.dataSize()) {
        LogEntry entry;
        size_t sz = readEntry(offset, entry);
        uint64_t h = dhtHashBytes(entry.key.data(), entry.key.size());
        order.push_back({h, entry.version()});
        verifyCrc(offset, sz);
        offset += sz;
    }

    ASSERT_EQ(order.size(), 5u);

    // Verify sorted by (hash ascending, version ascending)
    for (size_t i = 1; i < order.size(); ++i) {
        EXPECT_TRUE(order[i - 1].first < order[i].first ||
                    (order[i - 1].first == order[i].first &&
                     order[i - 1].second <= order[i].second))
            << "Entry " << i << " is out of order";
    }
}

TEST_F(FlushTest, RoundTrip) {
    WriteBuffer wb;
    wb.put("key1", 1, "val1", false);
    wb.put("key1", 2, "val2", false);
    wb.put("key2", 3, "val3", true);

    ASSERT_TRUE(seg_.create(path_, 1).ok());
    ASSERT_TRUE(wb.flush(seg_, 1, global_index_).ok());

    // Read all entries back and verify contents
    std::vector<LogEntry> entries;
    uint64_t offset = 0;
    while (offset < seg_.dataSize()) {
        LogEntry entry;
        size_t sz = readEntry(offset, entry);
        verifyCrc(offset, sz);
        entries.push_back(std::move(entry));
        offset += sz;
    }

    ASSERT_EQ(entries.size(), 3u);

    // Collect into a map for verification (key -> list of versions)
    std::map<std::string, std::vector<uint64_t>> by_key;
    for (const auto& e : entries) {
        by_key[e.key].push_back(e.version());
    }

    ASSERT_EQ(by_key.count("key1"), 1u);
    ASSERT_EQ(by_key["key1"].size(), 2u);
    // Versions should be sorted ascending within same key (same hash)
    EXPECT_LT(by_key["key1"][0], by_key["key1"][1]);

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
    EXPECT_EQ(seg_.entryCount(), 3u);
    EXPECT_EQ(seg_.keyCount(), 2u);

    // key1 should have 2 entries; latest version is 2
    LogEntry latest;
    ASSERT_TRUE(seg_.getLatest("key1", latest).ok());
    EXPECT_EQ(latest.version(), 2u);

    // key2 should have 1 entry
    ASSERT_TRUE(seg_.getLatest("key2", latest).ok());
    EXPECT_EQ(latest.version(), 3u);

    // Verify all entries for key1 via get
    std::vector<LogEntry> key1_entries;
    ASSERT_TRUE(seg_.get("key1", key1_entries).ok());
    ASSERT_EQ(key1_entries.size(), 2u);
    for (const auto& e : key1_entries) {
        EXPECT_EQ(e.key, "key1");
    }
}

TEST_F(FlushTest, SealAndOpen) {
    WriteBuffer wb;
    wb.put("alpha", 1, "v1", false);
    wb.put("alpha", 2, "v2", false);
    wb.put("beta", 10, "vb", true);

    ASSERT_TRUE(seg_.create(path_, 1).ok());
    ASSERT_TRUE(wb.flush(seg_, 1, global_index_).ok());
    uint64_t data_size = seg_.dataSize();
    seg_.close();

    // Open into a fresh Segment.
    Segment loaded;
    ASSERT_TRUE(loaded.open(path_).ok());

    // Verify data size matches (excludes index + footer).
    EXPECT_EQ(loaded.dataSize(), data_size);
    EXPECT_EQ(loaded.entryCount(), 3u);
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

    // Verify round-trip through get.
    std::vector<LogEntry> alpha_entries;
    ASSERT_TRUE(loaded.get("alpha", alpha_entries).ok());
    ASSERT_EQ(alpha_entries.size(), 2u);
    for (const auto& e : alpha_entries) {
        EXPECT_EQ(e.key, "alpha");
    }

    loaded.close();
}

TEST_F(FlushTest, GlobalIndexPopulated) {
    WriteBuffer wb;
    wb.put("key1", 1, "val1", false);
    wb.put("key1", 2, "val2", false);
    wb.put("key2", 3, "val3", true);
    wb.put("key3", 4, "val4", false);

    ASSERT_TRUE(seg_.create(path_, 77).ok());
    ASSERT_TRUE(wb.flush(seg_, 77, global_index_).ok());

    // Every entry should be registered in GlobalIndex with packed version.
    // Packed version = (logical_version << 1) | tombstone_bit.
    uint64_t version;
    uint32_t segment_id;
    ASSERT_TRUE(global_index_.getLatest("key1", version, segment_id).ok());
    EXPECT_EQ(version, 2u << 1);  // latest version for key1 (packed, non-tombstone)
    EXPECT_EQ(segment_id, 77u);

    ASSERT_TRUE(global_index_.getLatest("key2", version, segment_id).ok());
    EXPECT_EQ(version, (3u << 1) | 1);  // packed, tombstone
    EXPECT_EQ(segment_id, 77u);

    ASSERT_TRUE(global_index_.getLatest("key3", version, segment_id).ok());
    EXPECT_EQ(version, 4u << 1);  // packed, non-tombstone
    EXPECT_EQ(segment_id, 77u);

    EXPECT_TRUE(global_index_.getLatest("missing", version, segment_id).isNotFound());

    EXPECT_EQ(global_index_.keyCount(), 3u);
    EXPECT_EQ(global_index_.entryCount(), 4u);  // 4 total entries (key1 has 2 versions)

    // Verify all versions for key1 (packed).
    std::vector<uint32_t> seg_ids;
    std::vector<uint64_t> versions;
    ASSERT_TRUE(global_index_.get("key1", seg_ids, versions));
    ASSERT_EQ(seg_ids.size(), 2u);
    // Latest first.
    EXPECT_EQ(versions[0], 2u << 1);
    EXPECT_EQ(versions[1], 1u << 1);
    EXPECT_EQ(seg_ids[0], 77u);
    EXPECT_EQ(seg_ids[1], 77u);

    // Verify version-bounded get (upper_bound is packed).
    uint64_t ver64;
    ASSERT_TRUE(global_index_.get("key1", (1ULL << 1) | 1, ver64, segment_id));
    EXPECT_EQ(ver64, 1u << 1);
    EXPECT_EQ(segment_id, 77u);

    ASSERT_FALSE(global_index_.get("key1", (0ULL << 1) | 1, ver64, segment_id));
}
