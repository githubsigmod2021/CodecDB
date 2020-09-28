file(REMOVE_RECURSE
  "../../debug/libarrow.100.0.0.dylib"
  "../../debug/libarrow.100.dylib"
  "../../debug/libarrow.dylib"
  "../../debug/libarrow.pdb"
)

# Per-language clean rules from dependency scanning.
foreach(lang C CXX)
  include(CMakeFiles/arrow_shared.dir/cmake_clean_${lang}.cmake OPTIONAL)
endforeach()
