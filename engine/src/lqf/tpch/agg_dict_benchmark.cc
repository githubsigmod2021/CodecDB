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

BENCHMARK_F(AggBenchmark, Table)(benchmark::State &state) {
    for (auto _ : state) {
        auto table = ParquetTable::Open(LineItem::path, {LineItem::RECEIPTDATE});
        TableAgg agg(2560, [](DataRow &row) { return row(LineItem::RECEIPTDATE).asInt(); },
                     RowCopyFactory().field(F_RAW, LineItem::RECEIPTDATE, 0)->buildSnapshot(),
                     []() { return vector<AggField *>{new Count()}; });
        auto result = agg.agg(*table);
        size_ = result->size();
    }
}

BENCHMARK_F(AggBenchmark, String)(benchmark::State &state) {
    for (auto _ : state) {
//run your benchmark
        auto table = ParquetTable::Open(LineItem::path, {LineItem::RECEIPTDATE});
        HashStrAgg agg([](DataRow &row) {
                           ByteArray &date = row[LineItem::RECEIPTDATE].asByteArray();
                           return string((char *) date.ptr, date.len);
                       },
                       RowCopyFactory().field(F_STRING, LineItem::RECEIPTDATE, 0)->buildSnapshot(),
                       []() { return vector<AggField *>{new Count()}; });
        auto result = agg.agg(*table);
        size_ = result->size();
    }
}