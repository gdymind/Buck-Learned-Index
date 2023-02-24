cmake_minimum_required(VERSION 3.10)
project(BuckIndex)

# Download the google gtest
include(FetchContent)
FetchContent_Declare(googletest
  GIT_REPOSITORY https://github.com/google/googletest
  GIT_TAG release-1.12.1)
FetchContent_MakeAvailable(googletest)

include_directories(./include)
aux_source_directory(./src DIR_SRCS)

add_subdirectory(unittests)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O3 -pthread")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -pthread")
add_executable(BuckIndex main.cpp src/global.cpp include/bucket.h include/global.h include/segment.h)
