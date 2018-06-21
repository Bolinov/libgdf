cmake_minimum_required(VERSION 2.8.12)

cmake_policy(SET CMP0048 NEW)

project(arrow-download NONE)

include(ExternalProject)

set(ARROW_VERSION "apache-arrow-0.7.1")

if (NOT "$ENV{PARQUET_ARROW_VERSION}" STREQUAL "")
    set(ARROW_VERSION "$ENV{PARQUET_ARROW_VERSION}")
endif()

message(STATUS "Using Apache Arrow version: ${ARROW_VERSION}")

set(ARROW_URL "https://github.com/apache/arrow/archive/${ARROW_VERSION}.tar.gz")

set(ARROW_CMAKE_ARGS
    #Arrow dependencies
    -DARROW_WITH_LZ4=OFF
    -DARROW_WITH_ZSTD=OFF
    -DARROW_WITH_BROTLI=OFF
    -DARROW_WITH_SNAPPY=OFF
    -DARROW_WITH_ZLIB=OFF

    #Build settings
    -DARROW_BUILD_STATIC=ON
    -DARROW_BUILD_SHARED=OFF
    -DARROW_BOOST_USE_SHARED=OFF
    -DARROW_BUILD_TESTS=OFF
    -DARROW_TEST_MEMCHECK=OFF
    -DARROW_BUILD_BENCHMARKS=OFF

    #Arrow modules
    -DARROW_IPC=ON
    -DARROW_COMPUTE=OFF
    -DARROW_GPU=OFF
    -DARROW_JEMALLOC=OFF
    -DARROW_HDFS=OFF
    -DARROW_BOOST_VENDORED=OFF
    -DARROW_PYTHON=OFF


)

ExternalProject_Add(arrow
    URL                ${ARROW_URL}
    CONFIGURE_COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}" ${ARROW_CMAKE_ARGS} "${CMAKE_CURRENT_BINARY_DIR}/arrow-prefix/src/arrow/cpp/"
    INSTALL_COMMAND   make DESTDIR=${CMAKE_CURRENT_BINARY_DIR}/arrow-prefix/src/arrow-install install
)