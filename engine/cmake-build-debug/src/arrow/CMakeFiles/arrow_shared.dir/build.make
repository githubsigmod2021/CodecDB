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
include src/arrow/CMakeFiles/arrow_shared.dir/depend.make

# Include the progress variables for this target.
include src/arrow/CMakeFiles/arrow_shared.dir/progress.make

# Include the compile flags for this target's objects.
include src/arrow/CMakeFiles/arrow_shared.dir/flags.make

# Object files for target arrow_shared
arrow_shared_OBJECTS =

# External object files for target arrow_shared
arrow_shared_EXTERNAL_OBJECTS = \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/array.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/builder.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/array/builder_adaptive.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/array/builder_base.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/array/builder_binary.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/array/builder_decimal.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/array/builder_dict.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/array/builder_nested.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/array/builder_primitive.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/array/builder_union.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/array/concatenate.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/array/dict_internal.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/array/diff.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/array/validate.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/buffer.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/compare.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/extension_type.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/memory_pool.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/pretty_print.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/record_batch.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/result.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/scalar.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/sparse_tensor.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/status.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/table.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/table_builder.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/tensor.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/type.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/visitor.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/io/buffered.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/io/compressed.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/io/file.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/io/hdfs.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/io/hdfs_internal.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/io/interfaces.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/io/memory.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/io/slow.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/testing/util.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/util/basic_decimal.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/util/bit_util.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/util/compression.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/util/cpu_info.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/util/decimal.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/util/delimiting.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/util/formatting.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/util/int_util.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/util/io_util.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/util/iterator.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/util/logging.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/util/key_value_metadata.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/util/memory.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/util/parsing.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/util/string.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/util/string_builder.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/util/task_group.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/util/thread_pool.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/util/time.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/util/trie.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/util/uri.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/util/utf8.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/vendored/base64.cpp.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/vendored/datetime/tz.cpp.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/vendored/double-conversion/bignum.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/vendored/double-conversion/double-conversion.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/vendored/double-conversion/bignum-dtoa.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/vendored/double-conversion/fast-dtoa.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/vendored/double-conversion/cached-powers.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/vendored/double-conversion/fixed-dtoa.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/vendored/double-conversion/diy-fp.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/vendored/double-conversion/strtod.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/vendored/uriparser/UriCommon.c.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/vendored/uriparser/UriCompare.c.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/vendored/uriparser/UriEscape.c.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/vendored/uriparser/UriFile.c.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/vendored/uriparser/UriIp4Base.c.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/vendored/uriparser/UriIp4.c.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/vendored/uriparser/UriMemory.c.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/vendored/uriparser/UriNormalizeBase.c.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/vendored/uriparser/UriNormalize.c.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/vendored/uriparser/UriParseBase.c.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/vendored/uriparser/UriParse.c.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/vendored/uriparser/UriQuery.c.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/vendored/uriparser/UriRecompose.c.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/vendored/uriparser/UriResolve.c.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/vendored/uriparser/UriShorten.c.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/ipc/dictionary.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/ipc/feather.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/ipc/message.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/ipc/metadata_internal.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/ipc/options.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/ipc/reader.cc.o" \
"/Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_objlib.dir/ipc/writer.cc.o"

debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/array.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/builder.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/array/builder_adaptive.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/array/builder_base.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/array/builder_binary.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/array/builder_decimal.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/array/builder_dict.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/array/builder_nested.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/array/builder_primitive.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/array/builder_union.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/array/concatenate.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/array/dict_internal.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/array/diff.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/array/validate.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/buffer.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/compare.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/extension_type.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/memory_pool.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/pretty_print.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/record_batch.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/result.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/scalar.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/sparse_tensor.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/status.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/table.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/table_builder.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/tensor.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/type.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/visitor.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/io/buffered.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/io/compressed.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/io/file.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/io/hdfs.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/io/hdfs_internal.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/io/interfaces.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/io/memory.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/io/slow.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/testing/util.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/util/basic_decimal.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/util/bit_util.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/util/compression.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/util/cpu_info.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/util/decimal.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/util/delimiting.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/util/formatting.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/util/int_util.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/util/io_util.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/util/iterator.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/util/logging.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/util/key_value_metadata.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/util/memory.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/util/parsing.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/util/string.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/util/string_builder.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/util/task_group.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/util/thread_pool.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/util/time.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/util/trie.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/util/uri.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/util/utf8.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/vendored/base64.cpp.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/vendored/datetime/tz.cpp.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/vendored/double-conversion/bignum.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/vendored/double-conversion/double-conversion.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/vendored/double-conversion/bignum-dtoa.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/vendored/double-conversion/fast-dtoa.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/vendored/double-conversion/cached-powers.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/vendored/double-conversion/fixed-dtoa.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/vendored/double-conversion/diy-fp.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/vendored/double-conversion/strtod.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/vendored/uriparser/UriCommon.c.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/vendored/uriparser/UriCompare.c.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/vendored/uriparser/UriEscape.c.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/vendored/uriparser/UriFile.c.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/vendored/uriparser/UriIp4Base.c.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/vendored/uriparser/UriIp4.c.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/vendored/uriparser/UriMemory.c.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/vendored/uriparser/UriNormalizeBase.c.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/vendored/uriparser/UriNormalize.c.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/vendored/uriparser/UriParseBase.c.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/vendored/uriparser/UriParse.c.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/vendored/uriparser/UriQuery.c.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/vendored/uriparser/UriRecompose.c.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/vendored/uriparser/UriResolve.c.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/vendored/uriparser/UriShorten.c.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/ipc/dictionary.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/ipc/feather.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/ipc/message.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/ipc/metadata_internal.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/ipc/options.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/ipc/reader.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_objlib.dir/ipc/writer.cc.o
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_shared.dir/build.make
debug/libarrow.100.0.0.dylib: jemalloc_ep-prefix/src/jemalloc_ep/dist/lib/libjemalloc_pic.a
debug/libarrow.100.0.0.dylib: src/arrow/CMakeFiles/arrow_shared.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/Users/harper/git/CodecDB/engine/cmake-build-debug/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Linking CXX shared library ../../debug/libarrow.dylib"
	cd /Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/arrow_shared.dir/link.txt --verbose=$(VERBOSE)
	cd /Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow && $(CMAKE_COMMAND) -E cmake_symlink_library ../../debug/libarrow.100.0.0.dylib ../../debug/libarrow.100.dylib ../../debug/libarrow.dylib

debug/libarrow.100.dylib: debug/libarrow.100.0.0.dylib
	@$(CMAKE_COMMAND) -E touch_nocreate debug/libarrow.100.dylib

debug/libarrow.dylib: debug/libarrow.100.0.0.dylib
	@$(CMAKE_COMMAND) -E touch_nocreate debug/libarrow.dylib

# Rule to build all files generated by this target.
src/arrow/CMakeFiles/arrow_shared.dir/build: debug/libarrow.dylib

.PHONY : src/arrow/CMakeFiles/arrow_shared.dir/build

src/arrow/CMakeFiles/arrow_shared.dir/clean:
	cd /Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow && $(CMAKE_COMMAND) -P CMakeFiles/arrow_shared.dir/cmake_clean.cmake
.PHONY : src/arrow/CMakeFiles/arrow_shared.dir/clean

src/arrow/CMakeFiles/arrow_shared.dir/depend:
	cd /Users/harper/git/CodecDB/engine/cmake-build-debug && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /Users/harper/git/CodecDB/engine /Users/harper/git/CodecDB/engine/src/arrow /Users/harper/git/CodecDB/engine/cmake-build-debug /Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow /Users/harper/git/CodecDB/engine/cmake-build-debug/src/arrow/CMakeFiles/arrow_shared.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : src/arrow/CMakeFiles/arrow_shared.dir/depend

