file(REMOVE_RECURSE
  "../../debug/liblqf.100.0.0.dylib"
  "../../debug/liblqf.100.dylib"
  "../../debug/liblqf.dylib"
  "../../debug/liblqf.pdb"
)

# Per-language clean rules from dependency scanning.
foreach(lang CXX)
  include(CMakeFiles/lqf_shared.dir/cmake_clean_${lang}.cmake OPTIONAL)
endforeach()
