cmake_minimum_required(VERSION 2.8)

link_directories(${MISAKA_SOURCE_DIR})

add_executable(btree-test btree-test.c)
target_link_libraries(btree-test misaka)

# add_executable(document-test document-test.c)
# target_link_libraries(document-test misaka)

add_executable(btree-qsort-perf btree-qsort-perf.c)
target_link_libraries(btree-qsort-perf misaka)
