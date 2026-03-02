set(EXAMPLES
    basic
    snapshots
    batch
    iterator
)

foreach(example ${EXAMPLES})
    add_executable(kvlite_example_${example} ${example}.cpp)
    target_link_libraries(kvlite_example_${example} PRIVATE kvlite pthread)
endforeach()
