function(ADD_TPCH_EXEC NAME SOURCE)
    add_executable(${NAME} ${SOURCE})
    target_link_libraries(${NAME} PUBLIC tpch_static)
    target_compile_options(${NAME} PUBLIC ${LQF_SIMD_FLAGS})
endfunction()


set(TPCH_SRC
        tpchquery.cc
        query1.cc
        query2.cc
        query3.cc
        query4.cc
        query5.cc
        query6.cc
        query7.cc
        query8.cc
        query9.cc
        query10.cc
        query11.cc
        query12.cc
        query13.cc
        query14.cc
        query15.cc
        query16.cc
        query17.cc
        query18.cc
        query19.cc
        query20.cc
        query21.cc
        query22.cc
        )

add_library(tpch_static ${TPCH_SRC})
target_link_libraries(tpch_static PUBLIC lqf_static)
target_compile_options(tpch_static PUBLIC ${LQF_SIMD_FLAGS})

add_lqf_benchmark(tpchquery_benchmark)

add_lqf_benchmark(filter_onecol_benchmark)
add_lqf_benchmark(filter_twocol_benchmark)
add_lqf_benchmark(filter_in_benchmark)

add_lqf_benchmark(agg_dict_benchmark)
add_lqf_benchmark(agg_stripe_benchmark)

add_lqf_benchmark(join_con_benchmark)

add_tpch_exec(lqf-tpch-query-dev tpchquery_dev.cc)
add_tpch_exec(lqf-threadsanitizer-exp threadsanitizer_exp.cc)
add_tpch_exec(lqf-file-explore file_explore.cc)
