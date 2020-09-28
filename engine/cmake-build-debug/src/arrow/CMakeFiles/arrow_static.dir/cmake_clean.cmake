file(REMOVE_RECURSE
  "../../debug/libarrow.a"
  "../../debug/libarrow.pdb"
)

# Per-language clean rules from dependency scanning.
foreach(lang C CXX)
  include(CMakeFiles/arrow_static.dir/cmake_clean_${lang}.cmake OPTIONAL)
endforeach()
