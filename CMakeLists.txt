cmake_minimum_required(VERSION 2.8)

project(misaka)

set(CMAKE_C_FLAGS "-Wall")
set(CMAKE_CXX_FLAGS "-Wall")

set(CMAKE_C_FLAGS_RELEASE "-O2 -march=native -DNDEBUG -flto")
set(CMAKE_CXX_FLAGS_RELEASE "-O2 -march=native -DNDEBUG -flto")
set(CMAKE_LD_FLAGS_RELEASE "-flto")

add_library(misaka SHARED 
  misc.c
  btree-common.c
  btree-mem.c
  btree-cow.c
  operator.c
  match.c
  document.c
  util.c
  worker.c
  hashtable.c
  mempool.c
  core.c)

target_link_libraries(misaka pthread)

add_subdirectory(test)
add_subdirectory(test_driver)
