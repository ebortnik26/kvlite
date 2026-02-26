cmake_minimum_required(VERSION 3.14)

project(kvlite
    VERSION 0.1.0
    DESCRIPTION "A high-performance persistent key-value store for SSD drives"
    LANGUAGES CXX
)

# C++ Standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Options
option(KVLITE_BUILD_TESTS "Build kvlite tests" OFF)
option(KVLITE_BUILD_BENCHMARKS "Build kvlite benchmarks" OFF)
option(KVLITE_BUILD_EXAMPLES "Build kvlite examples" OFF)

# Compiler flags
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(-Wall -Wextra -Wpedantic)
    if(CMAKE_BUILD_TYPE STREQUAL "Release")
        add_compile_options(-O3)
    endif()
endif()

# Library sources
set(KVLITE_SOURCES
    src/db.cpp
    src/db_iterator.cpp
    src/status.cpp
    src/internal/bucket_arena.cpp
    src/internal/lslot_codec.cpp
    src/internal/delta_hash_table.cpp
    src/internal/read_only_delta_hash_table.cpp
    src/internal/read_write_delta_hash_table.cpp
    src/internal/global_index.cpp
    src/internal/segment_index.cpp
    src/internal/version_manager.cpp
    src/internal/write_buffer.cpp
    src/internal/log_file.cpp
    src/internal/segment.cpp
    src/internal/segment_storage_manager.cpp
    src/internal/manifest.cpp
    src/internal/global_index_wal.cpp
    src/internal/entry_stream.cpp
)

# Create library
add_library(kvlite ${KVLITE_SOURCES})

target_include_directories(kvlite
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
)

# Link pthread on Unix
if(UNIX)
    target_link_libraries(kvlite PRIVATE pthread)
endif()

# Tests
if(KVLITE_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

# Benchmarks
if(KVLITE_BUILD_BENCHMARKS)
    add_subdirectory(benchmarks)
endif()

# Examples
if(KVLITE_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

# Installation
include(GNUInstallDirs)

install(TARGETS kvlite
    EXPORT kvliteTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

install(DIRECTORY include/
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

install(EXPORT kvliteTargets
    FILE kvliteTargets.cmake
    NAMESPACE kvlite::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/kvlite
)
