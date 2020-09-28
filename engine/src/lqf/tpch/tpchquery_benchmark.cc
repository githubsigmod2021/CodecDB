//
// Created by harper on 3/2/20.
//

#include <benchmark/benchmark.h>
#include "tpchquery.h"

class TpchBenchmark : public benchmark::Fixture {
protected:
public:

    TpchBenchmark() {
    }

    virtual ~TpchBenchmark() {
    }
};


BENCHMARK_F(TpchBenchmark, Q1)(benchmark::State &state) {
    for (auto _ : state) {
        //run your benchmark
        lqf::tpch::executeQ1();
    }
}

//BENCHMARK_F(TpchBenchmark, Q2)(benchmark::State &state) {
//    for (auto _ : state) {
//    }
//}
