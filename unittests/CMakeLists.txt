cmake_minimum_required(VERSION 3.16)
project(unittests)

enable_testing()

file(GLOB unittests_src
  *.cc)

add_executable(unittests ${unittests_src})
target_link_libraries(unittests gtest gtest_main pthread)
include(GoogleTest)
#gtest_discover_tests(unittests) #commented this out to avoid unittest to be launched by make
