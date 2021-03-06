# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.17

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:


#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:


# Disable VCS-based implicit rules.
% : %,v


# Disable VCS-based implicit rules.
% : RCS/%


# Disable VCS-based implicit rules.
% : RCS/%,v


# Disable VCS-based implicit rules.
% : SCCS/s.%


# Disable VCS-based implicit rules.
% : s.%


.SUFFIXES: .hpux_make_needs_suffix_list


# Command-line flag to silence nested $(MAKE).
$(VERBOSE)MAKESILENT = -s

# Suppress display of executed commands.
$(VERBOSE).SILENT:


# A target that is always out of date.
cmake_force:

.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /Applications/CLion.app/Contents/bin/cmake/mac/bin/cmake

# The command to remove a file.
RM = /Applications/CLion.app/Contents/bin/cmake/mac/bin/cmake -E rm -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /Users/harper/git/CodecDB/engine

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /Users/harper/git/CodecDB/engine/cmake-build-debug

# Include any dependencies generated for this target.
include src/sboost/CMakeFiles/sboost_shared.dir/depend.make

# Include the progress variables for this target.
include src/sboost/CMakeFiles/sboost_shared.dir/progress.make

# Include the compile flags for this target's objects.
include src/sboost/CMakeFiles/sboost_shared.dir/flags.make

# Object files for target sboost_shared
sboost_shared_OBJECTS =

# External object files for target sboost_shared
sboost_shared_EXTERNAL_OBJECTS = \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/sboost/CMakeFiles/sboost_objlib.dir/bitmap_writer.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/sboost/CMakeFiles/sboost_objlib.dir/byteutils.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/sboost/CMakeFiles/sboost_objlib.dir/sboost.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/sboost/CMakeFiles/sboost_objlib.dir/unpacker.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/sboost/CMakeFiles/sboost_objlib.dir/encoding/deltabp.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/sboost/CMakeFiles/sboost_objlib.dir/encoding/encoding_utils.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/sboost/CMakeFiles/sboost_objlib.dir/encoding/rlehybrid.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/sboost/CMakeFiles/sboost_objlib.dir/simd.cc.o"

debug/libsboost.1.0.0.dylib: src/sboost/CMakeFiles/sboost_objlib.dir/bitmap_writer.cc.o
debug/libsboost.1.0.0.dylib: src/sboost/CMakeFiles/sboost_objlib.dir/byteutils.cc.o
debug/libsboost.1.0.0.dylib: src/sboost/CMakeFiles/sboost_objlib.dir/sboost.cc.o
debug/libsboost.1.0.0.dylib: src/sboost/CMakeFiles/sboost_objlib.dir/unpacker.cc.o
debug/libsboost.1.0.0.dylib: src/sboost/CMakeFiles/sboost_objlib.dir/encoding/deltabp.cc.o
debug/libsboost.1.0.0.dylib: src/sboost/CMakeFiles/sboost_objlib.dir/encoding/encoding_utils.cc.o
debug/libsboost.1.0.0.dylib: src/sboost/CMakeFiles/sboost_objlib.dir/encoding/rlehybrid.cc.o
debug/libsboost.1.0.0.dylib: src/sboost/CMakeFiles/sboost_objlib.dir/simd.cc.o
debug/libsboost.1.0.0.dylib: src/sboost/CMakeFiles/sboost_shared.dir/build.make
debug/libsboost.1.0.0.dylib: src/sboost/CMakeFiles/sboost_shared.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/Users/harper/git/CodecDB/engine/cmake-build-debug/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Linking CXX shared library ../../debug/libsboost.dylib"
	cd /Users/harper/git/CodecDB/engine/cmake-build-debug/src/sboost && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/sboost_shared.dir/link.txt --verbose=$(VERBOSE)
	cd /Users/harper/git/CodecDB/engine/cmake-build-debug/src/sboost && $(CMAKE_COMMAND) -E cmake_symlink_library ../../debug/libsboost.1.0.0.dylib ../../debug/libsboost.1.0.0.dylib ../../debug/libsboost.dylib

debug/libsboost.dylib: debug/libsboost.1.0.0.dylib
	@$(CMAKE_COMMAND) -E touch_nocreate debug/libsboost.dylib

# Rule to build all files generated by this target.
src/sboost/CMakeFiles/sboost_shared.dir/build: debug/libsboost.dylib

.PHONY : src/sboost/CMakeFiles/sboost_shared.dir/build

src/sboost/CMakeFiles/sboost_shared.dir/clean:
	cd /Users/harper/git/CodecDB/engine/cmake-build-debug/src/sboost && $(CMAKE_COMMAND) -P CMakeFiles/sboost_shared.dir/cmake_clean.cmake
.PHONY : src/sboost/CMakeFiles/sboost_shared.dir/clean

src/sboost/CMakeFiles/sboost_shared.dir/depend:
	cd /Users/harper/git/CodecDB/engine/cmake-build-debug && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /Users/harper/git/CodecDB/engine /Users/harper/git/CodecDB/engine/src/sboost /Users/harper/git/CodecDB/engine/cmake-build-debug /Users/harper/git/CodecDB/engine/cmake-build-debug/src/sboost /Users/harper/git/CodecDB/engine/cmake-build-debug/src/sboost/CMakeFiles/sboost_shared.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : src/sboost/CMakeFiles/sboost_shared.dir/depend

