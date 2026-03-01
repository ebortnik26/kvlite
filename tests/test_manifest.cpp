#include <gtest/gtest.h>

#include <cstring>
#include <filesystem>
#include <vector>
#include <fcntl.h>
#include <unistd.h>
#include <string>

#include "internal/crc32.h"
#include "internal/manifest.h"

using namespace kvlite::internal;
using kvlite::Status;

class ManifestTest : public ::testing::Test {
protected:
    void SetUp() override {
        path_ = "/tmp/manifest_test_" + std::to_string(getpid());
        std::filesystem::remove_all(path_);
    }
    void TearDown() override {
        std::filesystem::remove_all(path_);
    }
    std::string path_;
};

TEST_F(ManifestTest, CreateAndOpen) {
    Manifest m;
    ASSERT_FALSE(m.isOpen());
    ASSERT_TRUE(m.create(path_).ok());
    ASSERT_TRUE(m.isOpen());
    ASSERT_TRUE(m.close().ok());
    ASSERT_FALSE(m.isOpen());
}

TEST_F(ManifestTest, OpenNonExistentFails) {
    Manifest m;
    Status s = m.open(path_);
    EXPECT_TRUE(s.isNotFound());
}

TEST_F(ManifestTest, DoubleCreateFails) {
    Manifest m;
    ASSERT_TRUE(m.create(path_).ok());
    EXPECT_FALSE(m.create(path_).ok());
    ASSERT_TRUE(m.close().ok());
}

TEST_F(ManifestTest, SetAndGet) {
    Manifest m;
    ASSERT_TRUE(m.create(path_).ok());

    std::string val;
    EXPECT_FALSE(m.get("key1", val));

    ASSERT_TRUE(m.set("key1", "value1").ok());
    ASSERT_TRUE(m.get("key1", val));
    EXPECT_EQ(val, "value1");

    ASSERT_TRUE(m.set("key2", "value2").ok());
    ASSERT_TRUE(m.get("key2", val));
    EXPECT_EQ(val, "value2");

    ASSERT_TRUE(m.close().ok());
}

TEST_F(ManifestTest, SetThenRecover) {
    {
        Manifest m;
        ASSERT_TRUE(m.create(path_).ok());
        ASSERT_TRUE(m.set("k1", "v1").ok());
        ASSERT_TRUE(m.set("k2", "v2").ok());
        ASSERT_TRUE(m.close().ok());
    }
    {
        Manifest m;
        ASSERT_TRUE(m.open(path_).ok());
        std::string val;
        ASSERT_TRUE(m.get("k1", val));
        EXPECT_EQ(val, "v1");
        ASSERT_TRUE(m.get("k2", val));
        EXPECT_EQ(val, "v2");
        ASSERT_TRUE(m.close().ok());
    }
}

TEST_F(ManifestTest, OverwriteAndRecover) {
    {
        Manifest m;
        ASSERT_TRUE(m.create(path_).ok());
        ASSERT_TRUE(m.set("k1", "original").ok());
        ASSERT_TRUE(m.set("k1", "updated").ok());
        ASSERT_TRUE(m.close().ok());
    }
    {
        Manifest m;
        ASSERT_TRUE(m.open(path_).ok());
        std::string val;
        ASSERT_TRUE(m.get("k1", val));
        EXPECT_EQ(val, "updated");
        ASSERT_TRUE(m.close().ok());
    }
}

TEST_F(ManifestTest, CompactReducesFileSize) {
    Manifest m;
    ASSERT_TRUE(m.create(path_).ok());

    // Write many overwrites to accumulate log entries.
    for (int i = 0; i < 100; ++i) {
        ASSERT_TRUE(m.set("key", std::string(100, 'a' + (i % 26))).ok());
    }

    // Get file size before compaction.
    std::string manifest_path = path_ + "/MANIFEST";
    auto size_before = std::filesystem::file_size(manifest_path);

    ASSERT_TRUE(m.compact().ok());

    auto size_after = std::filesystem::file_size(manifest_path);
    EXPECT_LT(size_after, size_before);

    // Verify last value survived.
    std::string val;
    ASSERT_TRUE(m.get("key", val));
    EXPECT_EQ(val, std::string(100, 'a' + (99 % 26)));

    ASSERT_TRUE(m.close().ok());
}

TEST_F(ManifestTest, CompactThenRecover) {
    {
        Manifest m;
        ASSERT_TRUE(m.create(path_).ok());
        ASSERT_TRUE(m.set("k1", "v1").ok());
        ASSERT_TRUE(m.set("k2", "v2").ok());
        ASSERT_TRUE(m.set("k2", "v2_updated").ok());
        ASSERT_TRUE(m.compact().ok());
        ASSERT_TRUE(m.close().ok());
    }
    {
        Manifest m;
        ASSERT_TRUE(m.open(path_).ok());
        std::string val;
        ASSERT_TRUE(m.get("k1", val));
        EXPECT_EQ(val, "v1");
        ASSERT_TRUE(m.get("k2", val));
        EXPECT_EQ(val, "v2_updated");
        ASSERT_TRUE(m.close().ok());
    }
}

TEST_F(ManifestTest, PartialWriteRecovery) {
    // Write valid records, then append garbage to simulate a partial write.
    {
        Manifest m;
        ASSERT_TRUE(m.create(path_).ok());
        ASSERT_TRUE(m.set("k1", "v1").ok());
        ASSERT_TRUE(m.set("k2", "v2").ok());
        ASSERT_TRUE(m.close().ok());
    }

    // Append partial/garbage data to the file.
    std::string manifest_path = path_ + "/MANIFEST";
    int fd = ::open(manifest_path.c_str(), O_WRONLY | O_APPEND);
    ASSERT_GE(fd, 0);
    const char garbage[] = {'\x0a', '\x00', '\x00', '\x00', '\x01', '\xff'};
    ssize_t n = ::write(fd, garbage, sizeof(garbage));
    ASSERT_EQ(n, static_cast<ssize_t>(sizeof(garbage)));
    ::close(fd);

    // Recover should truncate the garbage and preserve valid records.
    {
        Manifest m;
        ASSERT_TRUE(m.open(path_).ok());
        std::string val;
        ASSERT_TRUE(m.get("k1", val));
        EXPECT_EQ(val, "v1");
        ASSERT_TRUE(m.get("k2", val));
        EXPECT_EQ(val, "v2");

        // Should be able to write new records after recovery.
        ASSERT_TRUE(m.set("k3", "v3").ok());
        ASSERT_TRUE(m.get("k3", val));
        EXPECT_EQ(val, "v3");

        ASSERT_TRUE(m.close().ok());
    }
}

TEST_F(ManifestTest, AppendAfterCompact) {
    Manifest m;
    ASSERT_TRUE(m.create(path_).ok());

    ASSERT_TRUE(m.set("k1", "v1").ok());
    ASSERT_TRUE(m.compact().ok());

    // Append after compaction should work.
    ASSERT_TRUE(m.set("k2", "v2").ok());

    std::string val;
    ASSERT_TRUE(m.get("k1", val));
    EXPECT_EQ(val, "v1");
    ASSERT_TRUE(m.get("k2", val));
    EXPECT_EQ(val, "v2");

    ASSERT_TRUE(m.close().ok());

    // Reopen and verify both records survived.
    {
        Manifest m2;
        ASSERT_TRUE(m2.open(path_).ok());
        ASSERT_TRUE(m2.get("k1", val));
        EXPECT_EQ(val, "v1");
        ASSERT_TRUE(m2.get("k2", val));
        EXPECT_EQ(val, "v2");
        ASSERT_TRUE(m2.close().ok());
    }
}

TEST_F(ManifestTest, EmptyRecovery) {
    // Create a manifest, close, reopen with no records.
    {
        Manifest m;
        ASSERT_TRUE(m.create(path_).ok());
        ASSERT_TRUE(m.close().ok());
    }
    {
        Manifest m;
        ASSERT_TRUE(m.open(path_).ok());
        std::string val;
        EXPECT_FALSE(m.get("anything", val));
        ASSERT_TRUE(m.close().ok());
    }
}

// Helper: build a raw manifest record (record_len + payload + crc32).
// Uses the same on-disk format as Manifest::appendRecord.
static std::vector<char> buildRawRecord(const std::string& key,
                                        const std::string& value) {
    static constexpr uint8_t kRecordTypeSet = 1;
    static constexpr size_t kRecordHeaderSize = 7;
    static constexpr size_t kRecordChecksumSize = 4;
    static constexpr size_t kRecordLenSize = 4;

    uint16_t key_len = static_cast<uint16_t>(key.size());
    uint32_t value_len = static_cast<uint32_t>(value.size());

    size_t payload_size = kRecordHeaderSize + key_len + value_len;
    std::vector<char> payload(payload_size);
    size_t off = 0;
    payload[off++] = static_cast<char>(kRecordTypeSet);
    std::memcpy(payload.data() + off, &key_len, 2); off += 2;
    std::memcpy(payload.data() + off, &value_len, 4); off += 4;
    std::memcpy(payload.data() + off, key.data(), key_len); off += key_len;
    std::memcpy(payload.data() + off, value.data(), value_len);

    uint32_t checksum = kvlite::internal::crc32(payload.data(), payload_size);
    uint32_t record_len = static_cast<uint32_t>(payload_size + kRecordChecksumSize);

    size_t total = kRecordLenSize + payload_size + kRecordChecksumSize;
    std::vector<char> record(total);
    size_t roff = 0;
    std::memcpy(record.data() + roff, &record_len, 4); roff += 4;
    std::memcpy(record.data() + roff, payload.data(), payload_size); roff += payload_size;
    std::memcpy(record.data() + roff, &checksum, 4);
    return record;
}

TEST_F(ManifestTest, CorruptedCrcRecovery) {
    // Write valid records, then append a record with a bad CRC.
    {
        Manifest m;
        ASSERT_TRUE(m.create(path_).ok());
        ASSERT_TRUE(m.set("k1", "v1").ok());
        ASSERT_TRUE(m.set("k2", "v2").ok());
        ASSERT_TRUE(m.close().ok());
    }

    // Build a valid record then corrupt its CRC.
    auto record = buildRawRecord("k3", "v3");
    // Flip a bit in the last byte (part of the CRC).
    record.back() ^= 0x01;

    std::string manifest_path = path_ + "/MANIFEST";
    int fd = ::open(manifest_path.c_str(), O_WRONLY | O_APPEND);
    ASSERT_GE(fd, 0);
    ssize_t n = ::write(fd, record.data(), record.size());
    ASSERT_EQ(n, static_cast<ssize_t>(record.size()));
    ::close(fd);

    // Recovery should truncate the bad-CRC record, preserve k1 and k2.
    {
        Manifest m;
        ASSERT_TRUE(m.open(path_).ok());
        std::string val;
        ASSERT_TRUE(m.get("k1", val));
        EXPECT_EQ(val, "v1");
        ASSERT_TRUE(m.get("k2", val));
        EXPECT_EQ(val, "v2");
        EXPECT_FALSE(m.get("k3", val));
        ASSERT_TRUE(m.close().ok());
    }
}

TEST_F(ManifestTest, TruncatedRecordRecovery) {
    // Write valid records, then append just a record_len prefix (no payload).
    {
        Manifest m;
        ASSERT_TRUE(m.create(path_).ok());
        ASSERT_TRUE(m.set("k1", "v1").ok());
        ASSERT_TRUE(m.close().ok());
    }

    // Append a record_len that claims more data than exists.
    std::string manifest_path = path_ + "/MANIFEST";
    int fd = ::open(manifest_path.c_str(), O_WRONLY | O_APPEND);
    ASSERT_GE(fd, 0);
    uint32_t fake_len = 100;  // Claims 100 bytes of payload+crc follow.
    ssize_t n = ::write(fd, &fake_len, sizeof(fake_len));
    ASSERT_EQ(n, static_cast<ssize_t>(sizeof(fake_len)));
    ::close(fd);

    // Recovery should truncate the incomplete record, preserve k1.
    {
        Manifest m;
        ASSERT_TRUE(m.open(path_).ok());
        std::string val;
        ASSERT_TRUE(m.get("k1", val));
        EXPECT_EQ(val, "v1");
        ASSERT_TRUE(m.close().ok());
    }
}

TEST_F(ManifestTest, CorruptedMiddleByte) {
    // Write valid records, then flip a byte in the last record's payload.
    {
        Manifest m;
        ASSERT_TRUE(m.create(path_).ok());
        ASSERT_TRUE(m.set("k1", "v1").ok());
        ASSERT_TRUE(m.set("k2", "v2").ok());
        ASSERT_TRUE(m.close().ok());
    }

    // Corrupt a byte near the end of the file (inside k2's record payload).
    std::string manifest_path = path_ + "/MANIFEST";
    auto file_size = std::filesystem::file_size(manifest_path);
    int fd = ::open(manifest_path.c_str(), O_RDWR);
    ASSERT_GE(fd, 0);
    // Flip a byte 8 bytes from the end (well within the last record's payload).
    off_t corrupt_pos = static_cast<off_t>(file_size) - 8;
    char byte;
    ASSERT_EQ(::pread(fd, &byte, 1, corrupt_pos), 1);
    byte ^= 0xFF;
    ASSERT_EQ(::pwrite(fd, &byte, 1, corrupt_pos), 1);
    ::close(fd);

    // Recovery should truncate the corrupted record, preserve k1.
    {
        Manifest m;
        ASSERT_TRUE(m.open(path_).ok());
        std::string val;
        ASSERT_TRUE(m.get("k1", val));
        EXPECT_EQ(val, "v1");
        EXPECT_FALSE(m.get("k2", val));
        ASSERT_TRUE(m.close().ok());
    }
}

TEST_F(ManifestTest, CreateFsyncDurability) {
    // Basic sanity: after create(), the MANIFEST file exists on disk.
    Manifest m;
    ASSERT_TRUE(m.create(path_).ok());

    std::string manifest_path = path_ + "/MANIFEST";
    EXPECT_TRUE(std::filesystem::exists(manifest_path));
    EXPECT_GE(std::filesystem::file_size(manifest_path), 8u);  // at least header

    ASSERT_TRUE(m.close().ok());
}

TEST_F(ManifestTest, CompactAfterCorruptSuffix) {
    // Write valid records, corrupt suffix, open (triggers recover + compact),
    // close, reopen, verify all valid data present.
    {
        Manifest m;
        ASSERT_TRUE(m.create(path_).ok());
        ASSERT_TRUE(m.set("k1", "v1").ok());
        ASSERT_TRUE(m.set("k2", "v2").ok());
        ASSERT_TRUE(m.set("k3", "v3").ok());
        ASSERT_TRUE(m.close().ok());
    }

    // Append garbage to corrupt the suffix.
    std::string manifest_path = path_ + "/MANIFEST";
    int fd = ::open(manifest_path.c_str(), O_WRONLY | O_APPEND);
    ASSERT_GE(fd, 0);
    const char garbage[] = {'\xDE', '\xAD', '\xBE', '\xEF', '\x00', '\x01'};
    ssize_t n = ::write(fd, garbage, sizeof(garbage));
    ASSERT_EQ(n, static_cast<ssize_t>(sizeof(garbage)));
    ::close(fd);

    auto size_before = std::filesystem::file_size(manifest_path);

    // Open triggers recover (truncates garbage) + compact (rewrites clean).
    {
        Manifest m;
        ASSERT_TRUE(m.open(path_).ok());
        std::string val;
        ASSERT_TRUE(m.get("k1", val)); EXPECT_EQ(val, "v1");
        ASSERT_TRUE(m.get("k2", val)); EXPECT_EQ(val, "v2");
        ASSERT_TRUE(m.get("k3", val)); EXPECT_EQ(val, "v3");
        ASSERT_TRUE(m.close().ok());
    }

    // File should be clean now (no garbage suffix).
    auto size_after = std::filesystem::file_size(manifest_path);
    EXPECT_LE(size_after, size_before);

    // Reopen again to verify compact saved a clean state.
    {
        Manifest m;
        ASSERT_TRUE(m.open(path_).ok());
        std::string val;
        ASSERT_TRUE(m.get("k1", val)); EXPECT_EQ(val, "v1");
        ASSERT_TRUE(m.get("k2", val)); EXPECT_EQ(val, "v2");
        ASSERT_TRUE(m.get("k3", val)); EXPECT_EQ(val, "v3");
        ASSERT_TRUE(m.close().ok());
    }
}
