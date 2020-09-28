#include <benchmark/benchmark.h>
#include <cstring>
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

//BENCHMARK_F(FilterBenchmark, Normal)(benchmark::State &state) {
//    for (auto _ : state) {
//        //
//        auto table = ParquetTable::Open(Part::path, {Part::CONTAINER});
//        ColFilter cf(new SimplePredicate(Part::CONTAINER, [=](const DataField &field) {
//            auto f = field.asByteArray();
//            return std::strncmp((const char *) f.ptr, "LG", 2) == 0;
//        }));
//        auto result = cf.filter(*table);
//        size_ = result->size();
//    }
//}

BENCHMARK_F(FilterBenchmark, LQF)(benchmark::State &state) {
    for (auto _ : state) {
        //run your benchmark
        auto table = ParquetTable::Open(Part::path, {Part::CONTAINER});
        ColFilter cf(
                new SboostPredicate<ByteArrayType>(Part::CONTAINER, std::bind(ByteArrayDictMultiEq::build, [](const ByteArray& key){
                    return std::strncmp((const char *) key.ptr, "LG", 2) == 0;
                })));
        auto result = cf.filter(*table);
        size_ = result->size();
        FilterExecutor::inst->reset();
    }
}
