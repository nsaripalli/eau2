cmake_minimum_required(VERSION 2.8.2)

# specify the C++ standard
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)

project(software-dev-as4)

cmake_minimum_required(VERSION 2.8.2)

# specify the C++ standard
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)

SET(GCC_COVERAGE_COMPILE_FLAGS "")
SET(GCC_COVERAGE_LINK_FLAGS    "-lpthread")

SET(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} ${GCC_COVERAGE_COMPILE_FLAGS}")
SET(CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS} ${GCC_COVERAGE_LINK_FLAGS}")

find_package(Threads REQUIRED)


add_executable(linus tests/linus.cpp)
add_executable(serialization tests/testSerialization.cpp)
add_executable(test_ddf tests/distributedDataFrameTests.cpp)
add_executable(wordCount tests/WordCount.cpp)
add_executable(server tests/server.cpp)
add_executable(m3 tests/m3.cpp)
add_executable(m4 tests/WordCount.cpp)
add_executable(personalTests tests/personal_test_suite.cpp)
add_executable(sor tests/testSor.cpp)