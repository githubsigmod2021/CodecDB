file(REMOVE_RECURSE
  "../../debug/liblqf.a"
  "../../debug/liblqf.pdb"
)

# Per-language clean rules from dependency scanning.
foreach(lang CXX)
  include(CMakeFiles/lqf_static.dir/cmake_clean_${lang}.cmake OPTIONAL)
endforeach()
