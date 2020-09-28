#----------------------------------------------------------------
# Generated CMake target import file for configuration "DEBUG".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "arrow_shared" for configuration "DEBUG"
set_property(TARGET arrow_shared APPEND PROPERTY IMPORTED_CONFIGURATIONS DEBUG)
set_target_properties(arrow_shared PROPERTIES
  IMPORTED_LOCATION_DEBUG "${_IMPORT_PREFIX}/lib/libarrow.100.0.0.dylib"
  IMPORTED_SONAME_DEBUG "@rpath/libarrow.100.dylib"
  )

list(APPEND _IMPORT_CHECK_TARGETS arrow_shared )
list(APPEND _IMPORT_CHECK_FILES_FOR_arrow_shared "${_IMPORT_PREFIX}/lib/libarrow.100.0.0.dylib" )

# Import target "arrow_static" for configuration "DEBUG"
set_property(TARGET arrow_static APPEND PROPERTY IMPORTED_CONFIGURATIONS DEBUG)
set_target_properties(arrow_static PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_DEBUG "C;CXX"
  IMPORTED_LOCATION_DEBUG "${_IMPORT_PREFIX}/lib/libarrow.a"
  )

list(APPEND _IMPORT_CHECK_TARGETS arrow_static )
list(APPEND _IMPORT_CHECK_FILES_FOR_arrow_static "${_IMPORT_PREFIX}/lib/libarrow.a" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
