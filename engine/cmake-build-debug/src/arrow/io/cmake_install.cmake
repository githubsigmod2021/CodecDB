# Install script for directory: /Users/harper/git/CodecDB/engine/src/arrow/io

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
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/arrow/io" TYPE FILE FILES
    "/Users/harper/git/CodecDB/engine/src/arrow/io/api.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/io/buffered.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/io/compressed.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/io/concurrency.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/io/file.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/io/hdfs.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/io/interfaces.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/io/memory.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/io/mman.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/io/slow.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/io/test_common.h"
    )
endif()

