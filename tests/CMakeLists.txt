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
    ${CMAKE_SOURCE_DIR}/src/internal/lslot_codec.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/delta_hash_table.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/global_index.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/segment_lslot_codec.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/segment_delta_hash_table.cpp
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
    ${CMAKE_SOURCE_DIR}/src/internal/segment_lslot_codec.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/segment_delta_hash_table.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/segment_index.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/global_index.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/gc.cpp
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

# WriteBuffer tests
add_executable(kvlite_wb_test
    test_write_buffer.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/write_buffer.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/segment.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/log_file.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/global_index.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/segment_index.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/segment_delta_hash_table.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/segment_lslot_codec.cpp
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
    ${CMAKE_SOURCE_DIR}/src/internal/segment_delta_hash_table.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/segment_lslot_codec.cpp
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
    ${CMAKE_SOURCE_DIR}/src/internal/segment_delta_hash_table.cpp
    ${CMAKE_SOURCE_DIR}/src/internal/segment_lslot_codec.cpp
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

# Register tests with CTest
include(GoogleTest)
gtest_discover_tests(kvlite_sm_test)
gtest_discover_tests(kvlite_test)
gtest_discover_tests(kvlite_bitstream_test)
gtest_discover_tests(kvlite_dht_test)
gtest_discover_tests(kvlite_segment_index_test)
gtest_discover_tests(kvlite_wb_test)
gtest_discover_tests(kvlite_seg_test)
gtest_discover_tests(kvlite_lf_test)
gtest_discover_tests(kvlite_vm_test)
gtest_discover_tests(kvlite_manifest_test)
