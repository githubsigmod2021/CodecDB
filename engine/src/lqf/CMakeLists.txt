# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
function(ADD_LQF_BENCHMARK REL_TEST_NAME)
    set(options)
    set(one_value_args PREFIX)
    set(multi_value_args)
    cmake_parse_arguments(ARG
            "${options}"
            "${one_value_args}"
            "${multi_value_args}"
            ${ARGN})
    if (ARG_PREFIX)
        set(PREFIX ${ARG_PREFIX})
    else ()
        set(PREFIX "lqf")
    endif ()
    add_benchmark(${REL_TEST_NAME}
            PREFIX
            ${PREFIX}
            LABELS
            "lqf-benchmarks"
            STATIC_LINK_LIBS
            ${LQF_BENCHMARK_LINK_LIBS}
            EXTRA_FLAGS
            ${LQF_SIMD_FLAGS}
            EXTRA_LINK_LIBS
            ${ARG_UNPARSED_ARGUMENTS})
endfunction()

set(LQF_SIMD_FLAGS -msse4.1 -mavx -mavx2 -mavx512f -mavx512bw -mavx512dq -mavx512vl -mbmi2)
set(LQF_BENCHMARK_LINK_LIBS benchmark::benchmark benchmark::benchmark_main Threads::Threads lqf_static)

set(LQF_SRCS
        threadpool.cc
        stream.cc
        agg.cc
        bitmap.cc
        concurrent.cc
        data_model.cc
        filter.cc
        filter_executor.cc
        join.cc
        print.cc
        sort.cc
        mat.cc
        util.cc
        union.cc
        dict.cc
        container.cc
        parallel.cc
        hash.cc
        hash_container.cc
        lang.cc
        memorypool.cc
        rowcopy.cc
        data_container.cc
        )

add_arrow_lib(lqf
        BUILD_STATIC
        BUILD_SHARED
        SOURCES
        ${LQF_SRCS}
        OUTPUTS
        LQF_LIBRARIES
        STATIC_LINK_LIBS
        parquet_static
        sboost_static
        SHARED_LINK_LIBS
        parquet_static
        sboost_static
        )
target_compile_options(lqf_objlib PUBLIC ${LQF_SIMD_FLAGS})
if(${LQF_PARALLEL})
    target_compile_definitions(lqf_objlib PUBLIC LQF_PARALLEL)
endif()


set(LQF_TEST_SRC
        bitmap_test.cc
        executor_test.cc
        stream_test.cc
        heap_test.cc
        data_model_test.cc
        filter_test.cc
        agg_test.cc
        join_test.cc
        print_test.cc
        sort_test.cc
        mat_test.cc
        container_test.cc
        parallel_test.cc
        rowcopy_test.cc
        dict_test.cc
        data_container_test.cc
        hash_container_test.cc
        )

add_test_case(all-test
        PREFIX
        lqf
        SOURCES
        ${LQF_TEST_SRC}
        STATIC_LINK_LIBS
        lqf_static
        GTest::GTest
        GTest::Main
        EXTRA_FLAGS
        ${LQF_SIMD_FLAGS}
        )

arrow_install_all_headers(lqf)

add_custom_target(lqf-benchmarks)
set(LQF_BENCHMARK_LINK_LIBS ${LQF_BENCHMARK_LINK_LIBS} tpch_static)

add_lqf_benchmark(container_benchmark)
add_lqf_benchmark(join_benchmark)
add_lqf_benchmark(bitmap_benchmark)
add_lqf_benchmark(stream_benchmark)
add_lqf_benchmark(rowcopy_benchmark)
add_lqf_benchmark(data_container_benchmark)
add_lqf_benchmark(hash_container_benchmark)

add_subdirectory(tpch)