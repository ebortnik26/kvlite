# Fetch Google Test
include(FetchContent)
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.14.0
)
FetchContent_MakeAvailable(googletest)

# Test executable (integration tests - requires full library implementation)
add_executable(kvlite_test
    test_basic.cpp
    test_batch.cpp
    test_versioning.cpp
    test_iterator.cpp
    test_concurrency.cpp
    test_db.cpp
    test_gc_daemon.cpp
)

target_include_directories(kvlite_test
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(kvlite_test
    PRIVATE
        kvlite
        GTest::gtest
        GTest::gtest_main
        pthread
)

# BitStream unit tests (header-only, no extra sources needed)
add_executable(kvlite_bitstream_test
    test_bit_stream.cpp
)

target_include_directories(kvlite_bitstream_test
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(kvlite_bitstream_test
    PRIVATE
        GTest::gtest
        GTest::gtest_main
)

# DHT unit tests (self-contained, only links DHT + GlobalIndex sources)
add_executable(kvlite_dht_test
    test_delta_hash_table.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/bucket_arena.cpp

    ${CMAKE_SOURCE_DIR}/src/internal/delta_hash_table.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/read_only_delta_hash_table.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/read_write_delta_hash_table.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/global_index.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/wal.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/global_index_wal.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/wal_stream.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/manifest.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/log_file.cpp
    ${CMAKE_SOURCE_DIR}/src/status.cpp
)

target_include_directories(kvlite_dht_test
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src
        ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(kvlite_dht_test
    PRIVATE
        GTest::gtest
        GTest::gtest_main
        pthread
)

# SegmentIndex tests
add_executable(kvlite_segment_index_test
    test_segment_index.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/bucket_arena.cpp

    ${CMAKE_SOURCE_DIR}/src/internal/delta_hash_table.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/read_only_delta_hash_table.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/read_write_delta_hash_table.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/segment_index.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/global_index.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/wal.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/global_index_wal.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/wal_stream.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/gc.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/entry_stream.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/memtable.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/log_file.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/segment.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/segment_storage_manager.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/manifest.cpp
    ${CMAKE_SOURCE_DIR}/src/status.cpp
)

target_include_directories(kvlite_segment_index_test
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src
        ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(kvlite_segment_index_test
    PRIVATE
        GTest::gtest
        GTest::gtest_main
        pthread
)

# WriteBuffer tests (Memtable unit tests + WriteBuffer orchestrator tests)
add_executable(kvlite_wb_test
    test_write_buffer.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/memtable.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/write_buffer.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/segment.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/log_file.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/bucket_arena.cpp

    ${CMAKE_SOURCE_DIR}/src/internal/delta_hash_table.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/read_only_delta_hash_table.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/segment_index.cpp
    ${CMAKE_SOURCE_DIR}/src/status.cpp
)

target_include_directories(kvlite_wb_test
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src
        ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(kvlite_wb_test
    PRIVATE
        GTest::gtest
        GTest::gtest_main
        pthread
)

# Segment tests
add_executable(kvlite_seg_test
    test_segment.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/segment.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/log_file.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/segment_index.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/bucket_arena.cpp

    ${CMAKE_SOURCE_DIR}/src/internal/delta_hash_table.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/read_only_delta_hash_table.cpp
    ${CMAKE_SOURCE_DIR}/src/status.cpp
)

target_include_directories(kvlite_seg_test
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src
        ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(kvlite_seg_test
    PRIVATE
        GTest::gtest
        GTest::gtest_main
        pthread
)

# LogFile tests
add_executable(kvlite_lf_test
    test_log_file.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/log_file.cpp
    ${CMAKE_SOURCE_DIR}/src/status.cpp
)

target_include_directories(kvlite_lf_test
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src
        ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(kvlite_lf_test
    PRIVATE
        GTest::gtest
        GTest::gtest_main
        pthread
)

# VersionManager tests
add_executable(kvlite_vm_test
    test_version_manager.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/version_manager.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/manifest.cpp
    ${CMAKE_SOURCE_DIR}/src/status.cpp
)

target_include_directories(kvlite_vm_test
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src
        ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(kvlite_vm_test
    PRIVATE
        GTest::gtest
        GTest::gtest_main
        pthread
)

# Manifest tests
add_executable(kvlite_manifest_test
    test_manifest.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/manifest.cpp
    ${CMAKE_SOURCE_DIR}/src/status.cpp
)

target_include_directories(kvlite_manifest_test
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src
        ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(kvlite_manifest_test
    PRIVATE
        GTest::gtest
        GTest::gtest_main
        pthread
)

# SegmentStorageManager tests
add_executable(kvlite_sm_test
    test_segment_storage_manager.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/segment_storage_manager.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/segment.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/log_file.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/segment_index.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/bucket_arena.cpp

    ${CMAKE_SOURCE_DIR}/src/internal/delta_hash_table.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/read_only_delta_hash_table.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/manifest.cpp
    ${CMAKE_SOURCE_DIR}/src/status.cpp
)

target_include_directories(kvlite_sm_test
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src
        ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(kvlite_sm_test
    PRIVATE
        GTest::gtest
        GTest::gtest_main
        pthread
)

# EntryStream tests
add_executable(kvlite_entry_stream_test
    test_entry_stream.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/entry_stream.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/gc.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/memtable.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/bucket_arena.cpp

    ${CMAKE_SOURCE_DIR}/src/internal/delta_hash_table.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/read_only_delta_hash_table.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/read_write_delta_hash_table.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/segment_index.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/global_index.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/wal.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/global_index_wal.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/wal_stream.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/manifest.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/log_file.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/segment.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/segment_storage_manager.cpp
    ${CMAKE_SOURCE_DIR}/src/status.cpp
)

target_include_directories(kvlite_entry_stream_test
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src
        ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(kvlite_entry_stream_test
    PRIVATE
        GTest::gtest
        GTest::gtest_main
        pthread
)

# WAL tests
add_executable(kvlite_wal_test
    test_wal.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/wal.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/log_file.cpp
    ${CMAKE_SOURCE_DIR}/src/status.cpp
)

target_include_directories(kvlite_wal_test
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src
        ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(kvlite_wal_test
    PRIVATE
        GTest::gtest
        GTest::gtest_main
        pthread
)

# WAL Stream tests
add_executable(kvlite_wal_stream_test
    test_wal_stream.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/wal_stream.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/wal.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/global_index_wal.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/manifest.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/log_file.cpp
    ${CMAKE_SOURCE_DIR}/src/status.cpp
)

target_include_directories(kvlite_wal_stream_test
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src
        ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(kvlite_wal_stream_test
    PRIVATE
        GTest::gtest
        GTest::gtest_main
        pthread
)

# Register test executables with CTest (one test per executable).
# This allows safe parallel execution with ctest -j2 since each executable
# uses its own temp directories.
add_test(NAME entry_stream_tests COMMAND kvlite_entry_stream_test)
add_test(NAME storage_manager_tests COMMAND kvlite_sm_test)
add_test(NAME integration_tests COMMAND kvlite_test)
add_test(NAME bitstream_tests COMMAND kvlite_bitstream_test)
add_test(NAME dht_tests COMMAND kvlite_dht_test)
add_test(NAME segment_index_tests COMMAND kvlite_segment_index_test)
add_test(NAME write_buffer_tests COMMAND kvlite_wb_test)
add_test(NAME segment_tests COMMAND kvlite_seg_test)
add_test(NAME log_file_tests COMMAND kvlite_lf_test)
add_test(NAME version_manager_tests COMMAND kvlite_vm_test)
add_test(NAME manifest_tests COMMAND kvlite_manifest_test)
add_test(NAME wal_tests COMMAND kvlite_wal_test)
add_test(NAME wal_stream_tests COMMAND kvlite_wal_stream_test)

# Default parallel level for ctest
set(CTEST_PARALLEL_LEVEL 4 CACHE STRING "Number of tests to run in parallel")
