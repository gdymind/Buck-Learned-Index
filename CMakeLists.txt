cmake_minimum_required(VERSION 3.10)
project(BuckIndex)
include_directories(./include)
aux_source_directory(./src DIR_SRCS)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O3 -pthread")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -pthread")
add_executable(BuckIndex main.cpp src/global.cpp include/bucket.h include/global.h include/segment.h)


# Locate GTest library
find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})

# Add test executable target
add_executable(my_test unittest/test.cpp)
target_link_libraries(my_test ${GTEST_LIBRARIES})

# Add test to CTest
include(CTest)
add_test(NAME my_test COMMAND my_test)
