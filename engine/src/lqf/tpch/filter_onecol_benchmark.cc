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
    ByteArray dateFrom("1998-09-01");
    for (auto _ : state) {
        //
        auto table = ParquetTable::Open(LineItem::path, {LineItem::SHIPDATE});
        ColFilter cf(new SimplePredicate(LineItem::SHIPDATE, [=](const DataField& field){
            return field.asByteArray() < dateFrom;
        }));
        auto result = cf.filter(*table);
        size_ = result->size();
    }
}

BENCHMARK_F(FilterBenchmark, LQF)(benchmark::State &state) {
    ByteArray dateFrom("1998-09-01");
    for (auto _ : state) {
        //run your benchmark
        auto table = ParquetTable::Open(LineItem::path, {LineItem::SHIPDATE});
        ColFilter cf(new SboostPredicate<ByteArrayType>(LineItem::SHIPDATE, std::bind(ByteArrayDictLess::build, dateFrom)));
        auto result = cf.filter(*table);
        size_ = result->size();
        FilterExecutor::inst->reset();
    }
}
