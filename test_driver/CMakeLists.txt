cmake_minimum_required(VERSION 2.8)

link_directories(${MISAKA_SOURCE_DIR})

add_executable(test-driver test.cpp)
target_link_libraries(test-driver misaka profiler)
