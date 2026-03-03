include(FetchContent)

FetchContent_Declare(
    datasketches
    GIT_REPOSITORY https://github.com/apache/datasketches-cpp.git
    GIT_TAG        5.0.2
)

set(BUILD_TESTS OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(datasketches)

add_executable(kvbench kvbench.cpp)

target_link_libraries(kvbench PRIVATE kvlite kll pthread)
