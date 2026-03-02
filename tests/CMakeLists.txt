# Fetch Google Test
include(FetchContent)
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.14.0
)
FetchContent_MakeAvailable(googletest)

# Integration tests — one executable per suite for parallel ctest runs.
# Each suite uses its own temp directory so they are safe to run concurrently.
set(INTEGRATION_TEST_SOURCES
    test_basic.cpp
    test_batch.cpp
    test_versioning.cpp
    test_iterator.cpp
    test_concurrency.cpp
    test_db.cpp
    test_gc_daemon.cpp
)

foreach(src ${INTEGRATION_TEST_SOURCES})
    get_filename_component(name ${src} NAME_WE)           # e.g. test_basic
    string(REPLACE "test_" "kvlite_int_" target ${name})  # e.g. kvlite_int_basic

    add_executable(${target} ${src})
    target_include_directories(${target} PRIVATE ${CMAKE_SOURCE_DIR}/src)
    target_link_libraries(${target} PRIVATE kvlite GTest::gtest GTest::gtest_main pthread)
endforeach()

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

# Register test executables with CTest.
# Integration suites are split into separate executables for parallel runs.

# Unit tests
add_test(NAME entry_stream_tests COMMAND kvlite_entry_stream_test)
add_test(NAME storage_manager_tests COMMAND kvlite_sm_test)
add_test(NAME bitstream_tests COMMAND kvlite_bitstream_test)
add_test(NAME segment_index_tests COMMAND kvlite_segment_index_test)
add_test(NAME write_buffer_tests COMMAND kvlite_wb_test)
add_test(NAME segment_tests COMMAND kvlite_seg_test)
add_test(NAME log_file_tests COMMAND kvlite_lf_test)
add_test(NAME version_manager_tests COMMAND kvlite_vm_test)
add_test(NAME manifest_tests COMMAND kvlite_manifest_test)
add_test(NAME wal_tests COMMAND kvlite_wal_test)
add_test(NAME wal_stream_tests COMMAND kvlite_wal_stream_test)

# DHT tests — split into shards by suite for parallel runs.
# RecoveryTest (~120s), SavepointTest (~100s), GlobalIndex+RW (~100s), rest (<1s)
add_test(NAME dht_recovery     COMMAND kvlite_dht_test --gtest_filter=RecoveryTest.*)
add_test(NAME dht_savepoint    COMMAND kvlite_dht_test --gtest_filter=SavepointTest.*)
add_test(NAME dht_global_index COMMAND kvlite_dht_test --gtest_filter=GlobalIndexDHT.*:ReadWriteDHT.*)
add_test(NAME dht_unit         COMMAND kvlite_dht_test --gtest_filter=-RecoveryTest.*:SavepointTest.*:GlobalIndexDHT.*:ReadWriteDHT.*)

# Integration tests (one per suite)
add_test(NAME basic_tests COMMAND kvlite_int_basic)
add_test(NAME batch_tests COMMAND kvlite_int_batch)
add_test(NAME versioning_tests COMMAND kvlite_int_versioning)
add_test(NAME iterator_tests COMMAND kvlite_int_iterator)
add_test(NAME db_tests COMMAND kvlite_int_db)
add_test(NAME gc_daemon_tests COMMAND kvlite_int_gc_daemon)

# Concurrency tests — split into two halves for parallel runs (~50s each).
add_test(NAME concurrency_1 COMMAND kvlite_int_concurrency
    --gtest_filter=ConcurrencyTest.ConcurrentPuts:ConcurrencyTest.ConcurrentPutsSameKey:ConcurrencyTest.ConcurrentReads:ConcurrencyTest.ConcurrentReadWrite:ConcurrencyTest.WriteBatchAtomicity:ConcurrencyTest.SnapshotIsolation:ConcurrencyTest.ConcurrentSnapshots:ConcurrencyTest.IteratorConsistencyDuringWrites)
add_test(NAME concurrency_2 COMMAND kvlite_int_concurrency
    --gtest_filter=ConcurrencyTest.VersionOrderingUnderConcurrency:ConcurrencyTest.StressTest:ConcurrencyTest.ReadBatchAtomicity:ConcurrencyTest.WriteBatchAllOrNothingViaReadBatch:ConcurrencyTest.SnapshotDoesNotSeeUncommittedVersion:ConcurrencyTest.SnapshotGetWaitsForInFlightPuts:ConcurrencyTest.SnapshotExistsWaitsForInFlightPuts:ConcurrencyTest.WriteBatchSameVersionUnderConcurrency)

# Default parallel level for ctest
set(CTEST_PARALLEL_LEVEL 8 CACHE STRING "Number of tests to run in parallel")
