cmake_minimum_required(VERSION 3.11)
project(BuckIndex)

set(CMAKE_CXX_STANDARD 17)
add_compile_options(-mavx -mavx2)

add_compile_options(-mpclmul)

# Download the google gtest
include(FetchContent)
FetchContent_Declare(googletest
  GIT_REPOSITORY https://github.com/google/googletest
  GIT_TAG release-1.12.1)
FetchContent_MakeAvailable(googletest)

include_directories(${CMAKE_SOURCE_DIR}/src)
include_directories(${CMAKE_SOURCE_DIR}/include)
include_directories(${CMAKE_SOURCE_DIR}/util)

add_subdirectory(unittests)


add_executable(BuckIndex main.cc)