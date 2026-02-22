#include <gtest/gtest.h>

#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include "internal/crc32.h"
#include "internal/delta_hash_table_base.h"
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

    ASSERT_TRUE(wb.getByVersion("key1", 3, value, version, tombstone));
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

    ASSERT_TRUE(wb.getByVersion("key1", 4, value, version, tombstone));
    EXPECT_EQ(value, "v3");
    EXPECT_EQ(version, 3u);
}

TEST(WriteBuffer, GetByVersionTooLow) {
    WriteBuffer wb;
    wb.put("key1", 5, "v5", false);

    std::string value;
    uint64_t version;
    bool tombstone;
    EXPECT_FALSE(wb.getByVersion("key1", 3, value, version, tombstone));
}

TEST(WriteBuffer, GetByVersionMissingKey) {
    WriteBuffer wb;
    std::string value;
    uint64_t version;
    bool tombstone;
    EXPECT_FALSE(wb.getByVersion("missing", 100, value, version, tombstone));
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

        uint64_t version;
        uint16_t key_len_raw;
        uint32_t vl;
        std::memcpy(&version, hdr, 8);
        std::memcpy(&key_len_raw, hdr + 8, 2);
        std::memcpy(&vl, hdr + 10, 4);

        bool tombstone = (key_len_raw & LogEntry::kTombstoneBit) != 0;
        uint32_t kl = key_len_raw & ~LogEntry::kTombstoneBit;

        out.pv = PackedVersion(version, tombstone);
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
    ASSERT_TRUE(wb.flush(path_, 1, seg_, global_index_).ok());
    EXPECT_EQ(seg_.dataSize(), 0u);
    EXPECT_EQ(seg_.entryCount(), 0u);
}

TEST_F(FlushTest, SingleEntry) {
    WriteBuffer wb;
    wb.put("hello", 42, "world", false);

    ASSERT_TRUE(wb.flush(path_, 1, seg_, global_index_).ok());

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

    ASSERT_TRUE(wb.flush(path_, 1, seg_, global_index_).ok());

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

    ASSERT_TRUE(wb.flush(path_, 1, seg_, global_index_).ok());

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

    // Verify sorted by (hash, version) ascending
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

    ASSERT_TRUE(wb.flush(path_, 1, seg_, global_index_).ok());

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

    ASSERT_TRUE(wb.flush(path_, 1, seg_, global_index_).ok());
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

    ASSERT_TRUE(wb.flush(path_, 77, seg_, global_index_).ok());

    // Every entry should be registered in GlobalIndex with segment_id and version.
    uint64_t version;
    uint32_t segment_id;
    ASSERT_TRUE(global_index_.getLatest("key1", version, segment_id));
    EXPECT_EQ(version, 2u);  // latest version for key1
    EXPECT_EQ(segment_id, 77u);

    ASSERT_TRUE(global_index_.getLatest("key2", version, segment_id));
    EXPECT_EQ(version, 3u);
    EXPECT_EQ(segment_id, 77u);

    ASSERT_TRUE(global_index_.getLatest("key3", version, segment_id));
    EXPECT_EQ(version, 4u);
    EXPECT_EQ(segment_id, 77u);

    EXPECT_FALSE(global_index_.getLatest("missing", version, segment_id));

    EXPECT_EQ(global_index_.keyCount(), 3u);
    EXPECT_EQ(global_index_.entryCount(), 4u);  // 4 total entries (key1 has 2 versions)

    // Verify all versions for key1.
    std::vector<uint32_t> seg_ids;
    std::vector<uint64_t> versions;
    ASSERT_TRUE(global_index_.get("key1", seg_ids, versions));
    ASSERT_EQ(seg_ids.size(), 2u);
    // Latest first.
    EXPECT_EQ(versions[0], 2u);
    EXPECT_EQ(versions[1], 1u);
    EXPECT_EQ(seg_ids[0], 77u);
    EXPECT_EQ(seg_ids[1], 77u);

    // Verify version-bounded get.
    uint64_t ver64;
    ASSERT_TRUE(global_index_.get("key1", 1u, ver64, segment_id));
    EXPECT_EQ(ver64, 1u);
    EXPECT_EQ(segment_id, 77u);

    ASSERT_FALSE(global_index_.get("key1", 0u, ver64, segment_id));
}
