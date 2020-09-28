file(REMOVE_RECURSE
  "../../debug/libsboost.1.0.0.dylib"
  "../../debug/libsboost.dylib"
  "../../debug/libsboost.pdb"
)

# Per-language clean rules from dependency scanning.
foreach(lang CXX)
  include(CMakeFiles/sboost_shared.dir/cmake_clean_${lang}.cmake OPTIONAL)
endforeach()
