# Fetch Google Test
include(FetchContent)
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.14.0
)
FetchContent_MakeAvailable(googletest)

# Test executable
add_executable(kvlite_test
    test_basic.cpp
    test_batch.cpp
    test_versioning.cpp
    test_iterator.cpp
    test_concurrency.cpp
)

target_link_libraries(kvlite_test
    PRIVATE
        kvlite
        GTest::gtest
        GTest::gtest_main
        pthread
)

# Register tests with CTest
include(GoogleTest)
gtest_discover_tests(kvlite_test)
