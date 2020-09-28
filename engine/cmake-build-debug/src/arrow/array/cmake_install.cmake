# Install script for directory: /Users/harper/git/CodecDB/engine/src/arrow/array

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
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/arrow/array" TYPE FILE FILES
    "/Users/harper/git/CodecDB/engine/src/arrow/array/builder_adaptive.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/array/builder_base.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/array/builder_binary.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/array/builder_decimal.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/array/builder_dict.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/array/builder_nested.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/array/builder_primitive.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/array/builder_time.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/array/builder_union.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/array/concatenate.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/array/diff.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/array/validate.h"
    )
endif()

