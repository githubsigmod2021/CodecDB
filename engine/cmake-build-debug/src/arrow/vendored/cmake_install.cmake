# Install script for directory: /Users/harper/git/CodecDB/engine/src/arrow/vendored

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
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/arrow/vendored" TYPE FILE FILES
    "/Users/harper/git/CodecDB/engine/src/arrow/vendored/datetime.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/vendored/optional.hpp"
    "/Users/harper/git/CodecDB/engine/src/arrow/vendored/string_view.hpp"
    "/Users/harper/git/CodecDB/engine/src/arrow/vendored/variant.hpp"
    "/Users/harper/git/CodecDB/engine/src/arrow/vendored/xxhash.h"
    )
endif()

if(NOT CMAKE_INSTALL_LOCAL_ONLY)
  # Include the install script for each subdirectory.
  include("/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/vendored/datetime/cmake_install.cmake")
  include("/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/vendored/double-conversion/cmake_install.cmake")

endif()

