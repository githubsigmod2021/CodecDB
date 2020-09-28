#include <benchmark/benchmark.h>
#include <lqf/data_model.h>
#include <lqf/filter.h>
#include <lqf/filter_executor.h>
#include "tpchquery.h"

class FilterBenchmark : public benchmark::Fixture {
protected:
    uint64_t size_;
public:

    FilterBenchmark() {
    }

    virtual ~FilterBenchmark() {
    }
};

using namespace lqf;
using namespace lqf::tpch;
using namespace lqf::sboost;

BENCHMARK_F(FilterBenchmark, Normal)(benchmark::State &state) {
    for (auto _ : state) {
        //
        auto table = ParquetTable::Open(LineItem::path, {LineItem::SHIPDATE,LineItem::RECEIPTDATE});
        RowFilter rf([](DataRow& row) {
            return row[LineItem::SHIPDATE].asByteArray() < row[LineItem::RECEIPTDATE].asByteArray();
        });
        auto result = rf.filter(*table);
        size_ = result->size();
    }
}

BENCHMARK_F(FilterBenchmark, LQF)(benchmark::State &state) {
    for (auto _ : state) {
        auto table = ParquetTable::Open(LineItem::path, {LineItem::SHIPDATE,LineItem::RECEIPTDATE});
        SboostRowFilter rf(LineItem::SHIPDATE,LineItem::RECEIPTDATE);
        auto result = rf.filter(*table);
        size_ = result->size();
    }
}
