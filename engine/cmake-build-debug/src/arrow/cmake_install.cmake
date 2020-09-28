# Install script for directory: /Users/harper/git/CodecDB/engine/src/arrow

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
    "/Users/harper/git/CodecDB/engine/cmake-build-debug/debug/libarrow.100.0.0.dylib"
    "/Users/harper/git/CodecDB/engine/cmake-build-debug/debug/libarrow.100.dylib"
    )
  foreach(file
      "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/libarrow.100.0.0.dylib"
      "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/libarrow.100.dylib"
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
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib" TYPE SHARED_LIBRARY FILES "/Users/harper/git/CodecDB/engine/cmake-build-debug/debug/libarrow.dylib")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/libarrow.dylib" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/libarrow.dylib")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/strip" -x "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/libarrow.dylib")
    endif()
  endif()
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib" TYPE STATIC_LIBRARY FILES "/Users/harper/git/CodecDB/engine/cmake-build-debug/debug/libarrow.a")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/libarrow.a" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/libarrow.a")
    execute_process(COMMAND "/Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/ranlib" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/libarrow.a")
  endif()
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/cmake/arrow" TYPE FILE FILES "/Users/harper/git/CodecDB/engine/cmake_modules/FindArrow.cmake")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/cmake/arrow/ArrowTargets.cmake")
    file(DIFFERENT EXPORT_FILE_CHANGED FILES
         "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/cmake/arrow/ArrowTargets.cmake"
         "/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/Export/lib/cmake/arrow/ArrowTargets.cmake")
    if(EXPORT_FILE_CHANGED)
      file(GLOB OLD_CONFIG_FILES "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/cmake/arrow/ArrowTargets-*.cmake")
      if(OLD_CONFIG_FILES)
        message(STATUS "Old export file \"$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/cmake/arrow/ArrowTargets.cmake\" will be replaced.  Removing files [${OLD_CONFIG_FILES}].")
        file(REMOVE ${OLD_CONFIG_FILES})
      endif()
    endif()
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/cmake/arrow" TYPE FILE FILES "/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/Export/lib/cmake/arrow/ArrowTargets.cmake")
  if("${CMAKE_INSTALL_CONFIG_NAME}" MATCHES "^([Dd][Ee][Bb][Uu][Gg])$")
    file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/cmake/arrow" TYPE FILE FILES "/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/Export/lib/cmake/arrow/ArrowTargets-debug.cmake")
  endif()
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/cmake/arrow" TYPE FILE FILES "/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/ArrowConfig.cmake")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/cmake/arrow" TYPE FILE FILES "/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/ArrowConfigVersion.cmake")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/pkgconfig" TYPE FILE FILES "/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/arrow.pc")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/arrow" TYPE FILE FILES
    "/Users/harper/git/CodecDB/engine/src/arrow/api.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/array.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/buffer.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/buffer_builder.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/builder.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/compare.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/extension_type.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/memory_pool.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/memory_pool_test.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/pretty_print.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/record_batch.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/result.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/scalar.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/sparse_tensor.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/status.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/stl.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/table.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/table_builder.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/tensor.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/type.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/type_fwd.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/type_traits.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/visitor.h"
    "/Users/harper/git/CodecDB/engine/src/arrow/visitor_inline.h"
    )
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/cmake/arrow" TYPE FILE FILES "/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/ArrowOptions.cmake")
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/cmake/arrow" TYPE FILE FILES "/Users/harper/git/CodecDB/engine/src/arrow/arrow-config.cmake")
endif()

if(NOT CMAKE_INSTALL_LOCAL_ONLY)
  # Include the install script for each subdirectory.
  include("/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/testing/cmake_install.cmake")
  include("/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/array/cmake_install.cmake")
  include("/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/io/cmake_install.cmake")
  include("/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/util/cmake_install.cmake")
  include("/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/vendored/cmake_install.cmake")
  include("/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/ipc/cmake_install.cmake")

endif()

