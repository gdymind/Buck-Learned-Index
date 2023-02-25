enable_testing()

file(GLOB unittests_src
  *.cc)

add_executable(unittests ${unittests_src})
target_link_libraries(unittests gtest_main)
include(GoogleTest)
gtest_discover_tests(unittests)
