function(ADD_SBOOST_BENCHMARK REL_TEST_NAME)
    set(options)
    set(one_value_args PREFIX)
    set(multi_value_args)
    cmake_parse_arguments(ARG
            "${options}"
            "${one_value_args}"
            "${multi_value_args}"
            ${ARGN})
    if(ARG_PREFIX)
        set(PREFIX ${ARG_PREFIX})
    else()
        set(PREFIX "sboost")
    endif()
    add_benchmark(${REL_TEST_NAME}
            PREFIX
            ${PREFIX}
            LABELS
            "sboost-benchmarks"
            ${SBOOST_BENCHMARK_LINK_OPTION}
            ${ARG_UNPARSED_ARGUMENTS})
endfunction()

set(SBOOST_VERSION 0.1)
set(SBOOST_SIMD_FLAGS -msse4.1 -mavx -mavx2 -mavx512f -mavx512bw -mavx512dq -mavx512vl -mbmi2)

set(SBOOST_SRC
        bitmap_writer.cc
        byteutils.cc
        sboost.cc
        unpacker.cc
        encoding/deltabp.cc
        encoding/encoding_utils.cc
        encoding/rlehybrid.cc
        simd.cc)

add_library(sboost_objlib OBJECT ${SBOOST_SRC})
set_property(TARGET sboost_objlib PROPERTY POSITION_INDEPENDENT_CODE 1)
target_compile_options(sboost_objlib PUBLIC -pthread -fvisibility=hidden ${SBOOST_SIMD_FLAGS})

add_library(sboost_static STATIC $<TARGET_OBJECTS:sboost_objlib>)
set_target_properties(sboost_static
        PROPERTIES LIBRARY_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}" OUTPUT_NAME sboost)
install(TARGETS sboost_static
        EXPORT sboost_targets
        RUNTIME DESTINATION ${RUNTIME_INSTALL_DIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

add_library(sboost_shared SHARED $<TARGET_OBJECTS:sboost_objlib>)
set_target_properties(sboost_shared
        PROPERTIES LIBRARY_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}" OUTPUT_NAME sboost)
set_target_properties(sboost_shared
        PROPERTIES LIBRARY_OUTPUT_DIRECTORY
        "${BUILD_OUTPUT_ROOT_DIRECTORY}"
        RUNTIME_OUTPUT_DIRECTORY
        "${BUILD_OUTPUT_ROOT_DIRECTORY}"
        OUTPUT_NAME
        sboost
        VERSION
        1.0.0
        SOVERSION
        1.0.0
        )

install(TARGETS sboost_shared
        EXPORT sboost_targets
        RUNTIME DESTINATION ${RUNTIME_INSTALL_DIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

set(TARGETS_CMAKE "SboostTargets.cmake")
install(EXPORT sboost_targets
        FILE "${TARGETS_CMAKE}"
        DESTINATION "${ARROW_CMAKE_INSTALL_DIR}")

set(CONFIG_CMAKE "SboostConfig.cmake")
set(BUILT_CONFIG_CMAKE "${CMAKE_CURRENT_BINARY_DIR}/${CONFIG_CMAKE}")
configure_package_config_file("${CONFIG_CMAKE}.in" "${BUILT_CONFIG_CMAKE}"
        INSTALL_DESTINATION "${ARROW_CMAKE_INSTALL_DIR}")
install(FILES "${BUILT_CONFIG_CMAKE}" DESTINATION "${ARROW_CMAKE_INSTALL_DIR}")

set(CONFIG_VERSION_CMAKE "SboostConfigVersion.cmake")
set(BUILT_CONFIG_VERSION_CMAKE "${CMAKE_CURRENT_BINARY_DIR}/${CONFIG_VERSION_CMAKE}")
write_basic_package_version_file("${BUILT_CONFIG_VERSION_CMAKE}"
        VERSION SBOOST_VERSION}
        COMPATIBILITY AnyNewerVersion)
install(FILES "${BUILT_CONFIG_VERSION_CMAKE}"
        DESTINATION "${ARROW_CMAKE_INSTALL_DIR}")


set(SBOOST_TEST_SRC
        bitmap_writer_test.cc
        byteutils_test.cc
        sboost_test.cc
        unpacker_test.cc
        encoding/rlehybrid_test.cc
        encoding/deltabp_test.cc)

add_test_case(all-test
        PREFIX
        sboost
        SOURCES
        ${SBOOST_TEST_SRC}
        STATIC_LINK_LIBS
        sboost_static
        GTest::GTest
        GTest::Main
        EXTRA_FLAGS
        ${SBOOST_SIMD_FLAGS}
        )
#target_compile_options(sboost-all-test PUBLIC ${SBOOST_SIMD_FLAGS})

add_custom_target(sboost-benchmarks)
set(SBOOST_BENCHMARK_LINK_OPTION STATIC_LINK_LIBS benchmark::benchmark benchmark::benchmark_main Threads::Threads
        sboost_static)
add_sboost_benchmark(unpacker_benchmark
        EXTRA_FLAGS
        ${SBOOST_SIMD_FLAGS})


arrow_install_cmake_find_module("${ARG_CMAKE_PACKAGE_NAME}")

