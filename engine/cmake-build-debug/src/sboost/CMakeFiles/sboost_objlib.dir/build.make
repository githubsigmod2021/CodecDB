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
include src/sboost/CMakeFiles/sboost_objlib.dir/depend.make

# Include the progress variables for this target.
include src/sboost/CMakeFiles/sboost_objlib.dir/progress.make

# Include the compile flags for this target's objects.
include src/sboost/CMakeFiles/sboost_objlib.dir/flags.make

src/sboost/CMakeFiles/sboost_objlib.dir/bitmap_writer.cc.o: src/sboost/CMakeFiles/sboost_objlib.dir/flags.make
src/sboost/CMakeFiles/sboost_objlib.dir/bitmap_writer.cc.o: ../src/sboost/bitmap_writer.cc
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/Users/harper/git/CodecDB/engine/cmake-build-debug/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object src/sboost/CMakeFiles/sboost_objlib.dir/bitmap_writer.cc.o"
	cd /Users/harper/git/CodecDB/engine/cmake-build-debug/src/sboost && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/sboost_objlib.dir/bitmap_writer.cc.o -c /Users/harper/git/CodecDB/engine/src/sboost/bitmap_writer.cc

src/sboost/CMakeFiles/sboost_objlib.dir/bitmap_writer.cc.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/sboost_objlib.dir/bitmap_writer.cc.i"
	cd /Users/harper/git/CodecDB/engine/cmake-build-debug/src/sboost && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /Users/harper/git/CodecDB/engine/src/sboost/bitmap_writer.cc > CMakeFiles/sboost_objlib.dir/bitmap_writer.cc.i

src/sboost/CMakeFiles/sboost_objlib.dir/bitmap_writer.cc.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/sboost_objlib.dir/bitmap_writer.cc.s"
	cd /Users/harper/git/CodecDB/engine/cmake-build-debug/src/sboost && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /Users/harper/git/CodecDB/engine/src/sboost/bitmap_writer.cc -o CMakeFiles/sboost_objlib.dir/bitmap_writer.cc.s

src/sboost/CMakeFiles/sboost_objlib.dir/byteutils.cc.o: src/sboost/CMakeFiles/sboost_objlib.dir/flags.make
src/sboost/CMakeFiles/sboost_objlib.dir/byteutils.cc.o: ../src/sboost/byteutils.cc
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/Users/harper/git/CodecDB/engine/cmake-build-debug/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Building CXX object src/sboost/CMakeFiles/sboost_objlib.dir/byteutils.cc.o"
	cd /Users/harper/git/CodecDB/engine/cmake-build-debug/src/sboost && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/sboost_objlib.dir/byteutils.cc.o -c /Users/harper/git/CodecDB/engine/src/sboost/byteutils.cc

src/sboost/CMakeFiles/sboost_objlib.dir/byteutils.cc.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/sboost_objlib.dir/byteutils.cc.i"
	cd /Users/harper/git/CodecDB/engine/cmake-build-debug/src/sboost && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /Users/harper/git/CodecDB/engine/src/sboost/byteutils.cc > CMakeFiles/sboost_objlib.dir/byteutils.cc.i

src/sboost/CMakeFiles/sboost_objlib.dir/byteutils.cc.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/sboost_objlib.dir/byteutils.cc.s"
	cd /Users/harper/git/CodecDB/engine/cmake-build-debug/src/sboost && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /Users/harper/git/CodecDB/engine/src/sboost/byteutils.cc -o CMakeFiles/sboost_objlib.dir/byteutils.cc.s

src/sboost/CMakeFiles/sboost_objlib.dir/sboost.cc.o: src/sboost/CMakeFiles/sboost_objlib.dir/flags.make
src/sboost/CMakeFiles/sboost_objlib.dir/sboost.cc.o: ../src/sboost/sboost.cc
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/Users/harper/git/CodecDB/engine/cmake-build-debug/CMakeFiles --progress-num=$(CMAKE_PROGRESS_3) "Building CXX object src/sboost/CMakeFiles/sboost_objlib.dir/sboost.cc.o"
	cd /Users/harper/git/CodecDB/engine/cmake-build-debug/src/sboost && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/sboost_objlib.dir/sboost.cc.o -c /Users/harper/git/CodecDB/engine/src/sboost/sboost.cc

src/sboost/CMakeFiles/sboost_objlib.dir/sboost.cc.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/sboost_objlib.dir/sboost.cc.i"
	cd /Users/harper/git/CodecDB/engine/cmake-build-debug/src/sboost && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /Users/harper/git/CodecDB/engine/src/sboost/sboost.cc > CMakeFiles/sboost_objlib.dir/sboost.cc.i

src/sboost/CMakeFiles/sboost_objlib.dir/sboost.cc.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/sboost_objlib.dir/sboost.cc.s"
	cd /Users/harper/git/CodecDB/engine/cmake-build-debug/src/sboost && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /Users/harper/git/CodecDB/engine/src/sboost/sboost.cc -o CMakeFiles/sboost_objlib.dir/sboost.cc.s

src/sboost/CMakeFiles/sboost_objlib.dir/unpacker.cc.o: src/sboost/CMakeFiles/sboost_objlib.dir/flags.make
src/sboost/CMakeFiles/sboost_objlib.dir/unpacker.cc.o: ../src/sboost/unpacker.cc
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/Users/harper/git/CodecDB/engine/cmake-build-debug/CMakeFiles --progress-num=$(CMAKE_PROGRESS_4) "Building CXX object src/sboost/CMakeFiles/sboost_objlib.dir/unpacker.cc.o"
	cd /Users/harper/git/CodecDB/engine/cmake-build-debug/src/sboost && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/sboost_objlib.dir/unpacker.cc.o -c /Users/harper/git/CodecDB/engine/src/sboost/unpacker.cc

src/sboost/CMakeFiles/sboost_objlib.dir/unpacker.cc.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/sboost_objlib.dir/unpacker.cc.i"
	cd /Users/harper/git/CodecDB/engine/cmake-build-debug/src/sboost && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /Users/harper/git/CodecDB/engine/src/sboost/unpacker.cc > CMakeFiles/sboost_objlib.dir/unpacker.cc.i

src/sboost/CMakeFiles/sboost_objlib.dir/unpacker.cc.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/sboost_objlib.dir/unpacker.cc.s"
	cd /Users/harper/git/CodecDB/engine/cmake-build-debug/src/sboost && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /Users/harper/git/CodecDB/engine/src/sboost/unpacker.cc -o CMakeFiles/sboost_objlib.dir/unpacker.cc.s

src/sboost/CMakeFiles/sboost_objlib.dir/encoding/deltabp.cc.o: src/sboost/CMakeFiles/sboost_objlib.dir/flags.make
src/sboost/CMakeFiles/sboost_objlib.dir/encoding/deltabp.cc.o: ../src/sboost/encoding/deltabp.cc
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/Users/harper/git/CodecDB/engine/cmake-build-debug/CMakeFiles --progress-num=$(CMAKE_PROGRESS_5) "Building CXX object src/sboost/CMakeFiles/sboost_objlib.dir/encoding/deltabp.cc.o"
	cd /Users/harper/git/CodecDB/engine/cmake-build-debug/src/sboost && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/sboost_objlib.dir/encoding/deltabp.cc.o -c /Users/harper/git/CodecDB/engine/src/sboost/encoding/deltabp.cc

src/sboost/CMakeFiles/sboost_objlib.dir/encoding/deltabp.cc.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/sboost_objlib.dir/encoding/deltabp.cc.i"
	cd /Users/harper/git/CodecDB/engine/cmake-build-debug/src/sboost && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /Users/harper/git/CodecDB/engine/src/sboost/encoding/deltabp.cc > CMakeFiles/sboost_objlib.dir/encoding/deltabp.cc.i

src/sboost/CMakeFiles/sboost_objlib.dir/encoding/deltabp.cc.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/sboost_objlib.dir/encoding/deltabp.cc.s"
	cd /Users/harper/git/CodecDB/engine/cmake-build-debug/src/sboost && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /Users/harper/git/CodecDB/engine/src/sboost/encoding/deltabp.cc -o CMakeFiles/sboost_objlib.dir/encoding/deltabp.cc.s

src/sboost/CMakeFiles/sboost_objlib.dir/encoding/encoding_utils.cc.o: src/sboost/CMakeFiles/sboost_objlib.dir/flags.make
src/sboost/CMakeFiles/sboost_objlib.dir/encoding/encoding_utils.cc.o: ../src/sboost/encoding/encoding_utils.cc
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/Users/harper/git/CodecDB/engine/cmake-build-debug/CMakeFiles --progress-num=$(CMAKE_PROGRESS_6) "Building CXX object src/sboost/CMakeFiles/sboost_objlib.dir/encoding/encoding_utils.cc.o"
	cd /Users/harper/git/CodecDB/engine/cmake-build-debug/src/sboost && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/sboost_objlib.dir/encoding/encoding_utils.cc.o -c /Users/harper/git/CodecDB/engine/src/sboost/encoding/encoding_utils.cc

src/sboost/CMakeFiles/sboost_objlib.dir/encoding/encoding_utils.cc.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/sboost_objlib.dir/encoding/encoding_utils.cc.i"
	cd /Users/harper/git/CodecDB/engine/cmake-build-debug/src/sboost && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /Users/harper/git/CodecDB/engine/src/sboost/encoding/encoding_utils.cc > CMakeFiles/sboost_objlib.dir/encoding/encoding_utils.cc.i

src/sboost/CMakeFiles/sboost_objlib.dir/encoding/encoding_utils.cc.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/sboost_objlib.dir/encoding/encoding_utils.cc.s"
	cd /Users/harper/git/CodecDB/engine/cmake-build-debug/src/sboost && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /Users/harper/git/CodecDB/engine/src/sboost/encoding/encoding_utils.cc -o CMakeFiles/sboost_objlib.dir/encoding/encoding_utils.cc.s

src/sboost/CMakeFiles/sboost_objlib.dir/encoding/rlehybrid.cc.o: src/sboost/CMakeFiles/sboost_objlib.dir/flags.make
src/sboost/CMakeFiles/sboost_objlib.dir/encoding/rlehybrid.cc.o: ../src/sboost/encoding/rlehybrid.cc
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/Users/harper/git/CodecDB/engine/cmake-build-debug/CMakeFiles --progress-num=$(CMAKE_PROGRESS_7) "Building CXX object src/sboost/CMakeFiles/sboost_objlib.dir/encoding/rlehybrid.cc.o"
	cd /Users/harper/git/CodecDB/engine/cmake-build-debug/src/sboost && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/sboost_objlib.dir/encoding/rlehybrid.cc.o -c /Users/harper/git/CodecDB/engine/src/sboost/encoding/rlehybrid.cc

src/sboost/CMakeFiles/sboost_objlib.dir/encoding/rlehybrid.cc.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/sboost_objlib.dir/encoding/rlehybrid.cc.i"
	cd /Users/harper/git/CodecDB/engine/cmake-build-debug/src/sboost && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /Users/harper/git/CodecDB/engine/src/sboost/encoding/rlehybrid.cc > CMakeFiles/sboost_objlib.dir/encoding/rlehybrid.cc.i

src/sboost/CMakeFiles/sboost_objlib.dir/encoding/rlehybrid.cc.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/sboost_objlib.dir/encoding/rlehybrid.cc.s"
	cd /Users/harper/git/CodecDB/engine/cmake-build-debug/src/sboost && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /Users/harper/git/CodecDB/engine/src/sboost/encoding/rlehybrid.cc -o CMakeFiles/sboost_objlib.dir/encoding/rlehybrid.cc.s

src/sboost/CMakeFiles/sboost_objlib.dir/simd.cc.o: src/sboost/CMakeFiles/sboost_objlib.dir/flags.make
src/sboost/CMakeFiles/sboost_objlib.dir/simd.cc.o: ../src/sboost/simd.cc
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/Users/harper/git/CodecDB/engine/cmake-build-debug/CMakeFiles --progress-num=$(CMAKE_PROGRESS_8) "Building CXX object src/sboost/CMakeFiles/sboost_objlib.dir/simd.cc.o"
	cd /Users/harper/git/CodecDB/engine/cmake-build-debug/src/sboost && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/sboost_objlib.dir/simd.cc.o -c /Users/harper/git/CodecDB/engine/src/sboost/simd.cc

src/sboost/CMakeFiles/sboost_objlib.dir/simd.cc.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/sboost_objlib.dir/simd.cc.i"
	cd /Users/harper/git/CodecDB/engine/cmake-build-debug/src/sboost && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /Users/harper/git/CodecDB/engine/src/sboost/simd.cc > CMakeFiles/sboost_objlib.dir/simd.cc.i

src/sboost/CMakeFiles/sboost_objlib.dir/simd.cc.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/sboost_objlib.dir/simd.cc.s"
	cd /Users/harper/git/CodecDB/engine/cmake-build-debug/src/sboost && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /Users/harper/git/CodecDB/engine/src/sboost/simd.cc -o CMakeFiles/sboost_objlib.dir/simd.cc.s

sboost_objlib: src/sboost/CMakeFiles/sboost_objlib.dir/bitmap_writer.cc.o
sboost_objlib: src/sboost/CMakeFiles/sboost_objlib.dir/byteutils.cc.o
sboost_objlib: src/sboost/CMakeFiles/sboost_objlib.dir/sboost.cc.o
sboost_objlib: src/sboost/CMakeFiles/sboost_objlib.dir/unpacker.cc.o
sboost_objlib: src/sboost/CMakeFiles/sboost_objlib.dir/encoding/deltabp.cc.o
sboost_objlib: src/sboost/CMakeFiles/sboost_objlib.dir/encoding/encoding_utils.cc.o
sboost_objlib: src/sboost/CMakeFiles/sboost_objlib.dir/encoding/rlehybrid.cc.o
sboost_objlib: src/sboost/CMakeFiles/sboost_objlib.dir/simd.cc.o
sboost_objlib: src/sboost/CMakeFiles/sboost_objlib.dir/build.make

.PHONY : sboost_objlib

# Rule to build all files generated by this target.
src/sboost/CMakeFiles/sboost_objlib.dir/build: sboost_objlib

.PHONY : src/sboost/CMakeFiles/sboost_objlib.dir/build

src/sboost/CMakeFiles/sboost_objlib.dir/clean:
	cd /Users/harper/git/CodecDB/engine/cmake-build-debug/src/sboost && $(CMAKE_COMMAND) -P CMakeFiles/sboost_objlib.dir/cmake_clean.cmake
.PHONY : src/sboost/CMakeFiles/sboost_objlib.dir/clean

src/sboost/CMakeFiles/sboost_objlib.dir/depend:
	cd /Users/harper/git/CodecDB/engine/cmake-build-debug && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /Users/harper/git/CodecDB/engine /Users/harper/git/CodecDB/engine/src/sboost /Users/harper/git/CodecDB/engine/cmake-build-debug /Users/harper/git/CodecDB/engine/cmake-build-debug/src/sboost /Users/harper/git/CodecDB/engine/cmake-build-debug/src/sboost/CMakeFiles/sboost_objlib.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : src/sboost/CMakeFiles/sboost_objlib.dir/depend
