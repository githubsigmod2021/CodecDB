# Install script for directory: /Users/harper/git/CodecDB/engine/src/lqf

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
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib" TYPE SHARED_LIBRARY FILES
    "/Users/harper/git/CodecDB/engine/cmake-build-debug/debug/liblqf.100.0.0.dylib"
    "/Users/harper/git/CodecDB/engine/cmake-build-debug/debug/liblqf.100.dylib"
    )
  foreach(file
      "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/liblqf.100.0.0.dylib"
      "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/liblqf.100.dylib"
      )
    if(EXISTS "${file}" AND
       NOT IS_SYMLINK "${file}")
      if(CMAKE_INSTALL_DO_STRIP)
        execute_process(COMMAND "/Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/strip" -x "${file}")
      endif()
    endif()
  endforeach()
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib" TYPE SHARED_LIBRARY FILES "/Users/harper/git/CodecDB/engine/cmake-build-debug/debug/liblqf.dylib")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/liblqf.dylib" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/liblqf.dylib")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/strip" -x "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/liblqf.dylib")
    endif()
  endif()
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib" TYPE STATIC_LIBRARY FILES "/Users/harper/git/CodecDB/engine/cmake-build-debug/debug/liblqf.a")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/liblqf.a" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/liblqf.a")
    execute_process(COMMAND "/Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/ranlib" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/liblqf.a")
  endif()
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/lqf" TYPE FILE FILES
    "/Users/harper/git/CodecDB/engine/src/lqf/agg.h"
    "/Users/harper/git/CodecDB/engine/src/lqf/bitmap.h"
    "/Users/harper/git/CodecDB/engine/src/lqf/concurrent.h"
    "/Users/harper/git/CodecDB/engine/src/lqf/container.h"
    "/Users/harper/git/CodecDB/engine/src/lqf/data_container.h"
    "/Users/harper/git/CodecDB/engine/src/lqf/data_model.h"
    "/Users/harper/git/CodecDB/engine/src/lqf/dict.h"
    "/Users/harper/git/CodecDB/engine/src/lqf/filter.h"
    "/Users/harper/git/CodecDB/engine/src/lqf/filter_executor.h"
    "/Users/harper/git/CodecDB/engine/src/lqf/hash.h"
    "/Users/harper/git/CodecDB/engine/src/lqf/hash_container.h"
    "/Users/harper/git/CodecDB/engine/src/lqf/heap.h"
    "/Users/harper/git/CodecDB/engine/src/lqf/join.h"
    "/Users/harper/git/CodecDB/engine/src/lqf/lang.h"
    "/Users/harper/git/CodecDB/engine/src/lqf/mat.h"
    "/Users/harper/git/CodecDB/engine/src/lqf/memorypool.h"
    "/Users/harper/git/CodecDB/engine/src/lqf/parallel.h"
    "/Users/harper/git/CodecDB/engine/src/lqf/print.h"
    "/Users/harper/git/CodecDB/engine/src/lqf/rowcopy.h"
    "/Users/harper/git/CodecDB/engine/src/lqf/sort.h"
    "/Users/harper/git/CodecDB/engine/src/lqf/stream.h"
    "/Users/harper/git/CodecDB/engine/src/lqf/test_util.h"
    "/Users/harper/git/CodecDB/engine/src/lqf/threadpool.h"
    "/Users/harper/git/CodecDB/engine/src/lqf/union.h"
    "/Users/harper/git/CodecDB/engine/src/lqf/util.h"
    "/Users/harper/git/CodecDB/engine/src/lqf/validate.h"
    )
endif()

if(NOT CMAKE_INSTALL_LOCAL_ONLY)
  # Include the install script for each subdirectory.
  include("/Users/harper/git/CodecDB/engine/cmake-build-debug/src/lqf/tpch/cmake_install.cmake")

endif()

