# Install script for directory: /Users/harper/git/CodecDB/engine/src/arrow/util

# Set the install prefix
if(NOT DEFINED CMAKE_INSTALL_PREFIX)
  set(CMAKE_INSTALL_PREFIX "/usr/local")
endif()
string(REGEX REPLACE "/$" "" CMAKE_INSTALL_PREFIX "${CMAKE_INSTALL_PREFIX}")

# Set the install configuration name.
if(NOT DEFINED CMAKE_INSTALL_CONFIG_NAME)
  if(BUILD_TYPE)
    string(REGEX REPLACE "^[^A-Za-z0-9_]+" ""
           CMAKE_INSTALL_CONFIG_NAME "${BUILD_TYPE}")
  else()
    set(CMAKE_INSTALL_CONFIG_NAME "DEBUG")
  endif()
  message(STATUS "Install configuration: \"${CMAKE_INSTALL_CONFIG_NAME}\"")
endif()

# Set the component getting installed.
if(NOT CMAKE_INSTALL_COMPONENT)
  if(COMPONENT)
    message(STATUS "Install component: \"${COMPONENT}\"")
    set(CMAKE_INSTALL_COMPONENT "${COMPONENT}")
  else()
    set(CMAKE_INSTALL_COMPONENT)
  endif()
endif()

# Is this installation the result of a crosscompile?
if(NOT DEFINED CMAKE_CROSSCOMPILING)
  set(CMAKE_CROSSCOMPILING "FALSE")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/arrow/util" TYPE FILE FILES
    "/Users/harper/git/CodecDB/engine/src/arrow/util/align_util.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/atomic_shared_ptr.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/base64.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/basic_decimal.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/bit_stream_utils.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/bit_util.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/bpacking.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/checked_cast.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/compare.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/compiler_util.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/compression.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/compression_brotli.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/compression_bz2.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/compression_lz4.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/compression_snappy.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/compression_zlib.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/compression_zstd.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/cpu_info.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/decimal.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/delimiting.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/double_conversion.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/formatting.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/functional.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/hash_util.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/hashing.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/int_util.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/io_util.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/iterator.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/key_value_metadata.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/logging.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/macros.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/make_unique.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/memory.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/neon_util.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/optional.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/parallel.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/parsing.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/print.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/range.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/rle_encoding.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/sort.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/sse_util.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/stopwatch.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/string.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/string_builder.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/string_view.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/task_group.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/thread_pool.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/time.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/trie.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/type_traits.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/ubsan.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/uri.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/utf8.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/variant.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/vector.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/visibility.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/util/windows_compatibility.h"
    )
endif()

