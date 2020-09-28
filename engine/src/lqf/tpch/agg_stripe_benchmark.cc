//
// Created by harper on 7/16/20.
//
#include <benchmark/benchmark.h>
#include <lqf/agg.h>
#include <lqf/join.h>
#include "tpchquery.h"

class AggBenchmark : public benchmark::Fixture {
protected:
    uint64_t size_;
public:

    AggBenchmark() {
    }

    virtual ~AggBenchmark() {
    }
};

using namespace lqf;
using namespace lqf::tpch;

BENCHMARK_F(AggBenchmark, Stripe)(benchmark::State &state) {
    for (auto _ : state) {
        auto table = ParquetTable::Open(Orders::path,{Orders::CUSTKEY});
        StripeHashAgg agg(32, COL_HASHER(Orders::CUSTKEY), COL_HASHER(0),
                                   RowCopyFactory().field(F_REGULAR, Orders::CUSTKEY, 0)->buildSnapshot(),
                                   RowCopyFactory().field(F_REGULAR, 0, 0)->buildSnapshot(),
                                   []() { return vector<AggField *>{new Count()}; });
        auto result = agg.agg(*table);
        size_ = result->size();
    }
}

BENCHMARK_F(AggBenchmark, Normal)(benchmark::State &state) {
    for (auto _ : state) {
//run your benchmark
        auto table = ParquetTable::Open(Orders::path,{Orders::CUSTKEY});
        HashAgg agg(COL_HASHER(Orders::CUSTKEY),
                          RowCopyFactory().field(F_REGULAR, Orders::CUSTKEY, 0)->buildSnapshot(),
                          []() { return vector<AggField *>{new Count()}; });
        auto result = agg.agg(*table);
        size_ = result->size();
    }
}

BENCHMARK_F(AggBenchmark, Large)(benchmark::State &state) {
    for (auto _ : state) {
//run your benchmark
        auto table = ParquetTable::Open(Orders::path,{Orders::CUSTKEY});
        HashLargeAgg agg(COL_HASHER(Orders::CUSTKEY),
                    RowCopyFactory().field(F_REGULAR, Orders::CUSTKEY, 0)->buildSnapshot(),
                    []() { return vector<AggField *>{new Count()}; });
        auto result = agg.agg(*table);
        size_ = result->size();
    }
}

