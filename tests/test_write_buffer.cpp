#include <gtest/gtest.h>

#include <cstdio>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "internal/crc32.h"
#include "internal/delta_hash_table_base.h"
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

    // Read one LogEntry from a given offset, return bytes consumed.
    size_t readEntry(uint64_t offset, LogEntry& out) {
        uint8_t hdr[LogEntry::kHeaderSize];
        EXPECT_TRUE(seg_.readAt(offset, hdr, LogEntry::kHeaderSize).ok());

        uint64_t pv_data;
        uint32_t kl, vl;
        std::memcpy(&pv_data, hdr, 8);
        std::memcpy(&kl, hdr + 8, 4);
        std::memcpy(&vl, hdr + 12, 4);

        out.pv = PackedVersion(pv_data);
        out.key.resize(kl);
        out.value.resize(vl);

        if (kl > 0) {
            EXPECT_TRUE(seg_.readAt(offset + LogEntry::kHeaderSize, out.key.data(), kl).ok());
        }
        if (vl > 0) {
            EXPECT_TRUE(seg_.readAt(offset + LogEntry::kHeaderSize + kl, out.value.data(), vl).ok());
        }

        return LogEntry::kHeaderSize + kl + vl + LogEntry::kChecksumSize;
    }

    // Verify CRC of an entry at a given offset with known total size.
    void verifyCrc(uint64_t offset, size_t total_size) {
        std::vector<uint8_t> buf(total_size);
        ASSERT_TRUE(seg_.readAt(offset, buf.data(), total_size).ok());
        size_t payload_len = total_size - LogEntry::kChecksumSize;
        uint32_t expected;
        std::memcpy(&expected, buf.data() + payload_len, 4);
        uint32_t actual = crc32(buf.data(), payload_len);
        EXPECT_EQ(actual, expected);
    }

    std::string path_;
    Segment seg_;
};

} // namespace

TEST_F(FlushTest, EmptyBuffer) {
    WriteBuffer wb;
    Status s = wb.flush(path_, seg_);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(seg_.size(), 0u);
    EXPECT_EQ(seg_.entryCount(), 0u);
}

TEST_F(FlushTest, SingleEntry) {
    WriteBuffer wb;
    wb.put("hello", 42, "world", false);

    Status s = wb.flush(path_, seg_);
    ASSERT_TRUE(s.ok());

    size_t expected_size = LogEntry::kHeaderSize + 5 + 5 + LogEntry::kChecksumSize;
    EXPECT_EQ(seg_.size(), expected_size);

    LogEntry entry;
    size_t consumed = readEntry(0, entry);
    EXPECT_EQ(consumed, expected_size);
    EXPECT_EQ(entry.key, "hello");
    EXPECT_EQ(entry.value, "world");
    EXPECT_EQ(entry.version(), 42u);
    EXPECT_FALSE(entry.tombstone());

    verifyCrc(0, expected_size);

    // Verify L2 index
    EXPECT_EQ(seg_.entryCount(), 1u);
    uint32_t off32, ver32;
    ASSERT_TRUE(seg_.getLatest("hello", off32, ver32));
    EXPECT_EQ(off32, 0u);
    EXPECT_EQ(ver32, 42u);
}

TEST_F(FlushTest, TombstoneEntry) {
    WriteBuffer wb;
    wb.put("deleted", 7, "", true);

    ASSERT_TRUE(wb.flush(path_, seg_).ok());

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

    ASSERT_TRUE(wb.flush(path_, seg_).ok());

    // Read all entries back
    std::vector<std::pair<uint64_t, uint64_t>> order; // (hash, version)
    uint64_t offset = 0;
    while (offset < seg_.size()) {
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

    ASSERT_TRUE(wb.flush(path_, seg_).ok());

    // Read all entries back and verify contents
    std::vector<LogEntry> entries;
    uint64_t offset = 0;
    while (offset < seg_.size()) {
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

    // Verify L2 index was populated correctly
    EXPECT_EQ(seg_.entryCount(), 3u);
    EXPECT_EQ(seg_.keyCount(), 2u);

    // key1 should have 2 entries; latest version is 2
    uint32_t off32, ver32;
    ASSERT_TRUE(seg_.getLatest("key1", off32, ver32));
    EXPECT_EQ(ver32, 2u);

    // key2 should have 1 entry
    ASSERT_TRUE(seg_.getLatest("key2", off32, ver32));
    EXPECT_EQ(ver32, 3u);

    // L2 offsets should point to valid entries we can read back
    std::vector<uint32_t> offsets, versions;
    ASSERT_TRUE(seg_.get("key1", offsets, versions));
    ASSERT_EQ(offsets.size(), 2u);
    for (size_t i = 0; i < offsets.size(); ++i) {
        LogEntry le;
        readEntry(offsets[i], le);
        EXPECT_EQ(le.key, "key1");
        EXPECT_EQ(le.version(), versions[i]);
    }
}

TEST_F(FlushTest, SaveAndLoad) {
    WriteBuffer wb;
    wb.put("alpha", 1, "v1", false);
    wb.put("alpha", 2, "v2", false);
    wb.put("beta", 10, "vb", true);

    ASSERT_TRUE(wb.flush(path_, seg_).ok());

    // Save the index to a separate file.
    std::string idx_path = path_ + ".idx";
    ASSERT_TRUE(seg_.saveIndex(idx_path).ok());
    seg_.close();

    // Load into a fresh Segment.
    Segment loaded;
    ASSERT_TRUE(loaded.load(path_, idx_path).ok());

    // Verify stats match the original.
    EXPECT_EQ(loaded.entryCount(), 3u);
    EXPECT_EQ(loaded.keyCount(), 2u);

    // Verify index lookups work.
    uint32_t off32, ver32;
    ASSERT_TRUE(loaded.getLatest("alpha", off32, ver32));
    EXPECT_EQ(ver32, 2u);

    ASSERT_TRUE(loaded.getLatest("beta", off32, ver32));
    EXPECT_EQ(ver32, 10u);

    // Verify data can be read back through the loaded segment.
    uint8_t hdr[LogEntry::kHeaderSize];
    ASSERT_TRUE(loaded.readAt(off32, hdr, LogEntry::kHeaderSize).ok());
    uint32_t kl;
    std::memcpy(&kl, hdr + 8, 4);
    std::string key(kl, '\0');
    ASSERT_TRUE(loaded.readAt(off32 + LogEntry::kHeaderSize, key.data(), kl).ok());
    EXPECT_EQ(key, "beta");

    // Verify round-trip through index offsets.
    std::vector<uint32_t> offsets, versions;
    ASSERT_TRUE(loaded.get("alpha", offsets, versions));
    ASSERT_EQ(offsets.size(), 2u);
    for (size_t i = 0; i < offsets.size(); ++i) {
        uint8_t h[LogEntry::kHeaderSize];
        ASSERT_TRUE(loaded.readAt(offsets[i], h, LogEntry::kHeaderSize).ok());
        uint64_t pv_data;
        std::memcpy(&pv_data, h, 8);
        PackedVersion pv(pv_data);
        EXPECT_EQ(pv.version(), versions[i]);
    }

    loaded.close();
    std::remove(idx_path.c_str());
}
