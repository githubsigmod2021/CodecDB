#include <benchmark/benchmark.h>
#include <cstring>
#include <lqf/data_model.h>
#include <lqf/filter.h>
#include <lqf/join.h>
#include <lqf/filter_executor.h>
#include "tpchquery.h"

class JoinBenchmark : public benchmark::Fixture {
protected:
    uint64_t size_;
public:

    JoinBenchmark() {
    }

    virtual ~JoinBenchmark() {
    }
};

using namespace lqf;
using namespace lqf::tpch;
using namespace lqf::sboost;

BENCHMARK_F(JoinBenchmark, Filter)(benchmark::State &state) {
    ByteArray segment("HOUSEHOLD");
    for (auto _ : state) {
        //run your benchmark
        auto customerTable = ParquetTable::Open(Customer::path, {Customer::MKTSEGMENT,Customer::CUSTKEY});

        ColFilter custFilter(
                {new SboostPredicate<ByteArrayType>(Customer::MKTSEGMENT, bind(&ByteArrayDictEq::build, segment))});
        auto filteredCustTable = custFilter.filter(*customerTable);
        size_ = filteredCustTable->size();
        FilterExecutor::inst->reset();
    }
}

BENCHMARK_F(JoinBenchmark, Normal)(benchmark::State &state) {
    ByteArray segment("HOUSEHOLD");
    for (auto _ : state) {
        //run your benchmark
        auto customerTable = ParquetTable::Open(Customer::path, {Customer::MKTSEGMENT, Customer::CUSTKEY});
        auto orderTable = ParquetTable::Open(Orders::path, {Orders::CUSTKEY});

        ColFilter custFilter(
                {new SboostPredicate<ByteArrayType>(Customer::MKTSEGMENT, bind(&ByteArrayDictEq::build, segment))});
        auto filteredCustTable = custFilter.filter(*customerTable);

        unordered_set<int32_t> container;
        filteredCustTable->blocks()->sequential()->foreach([&container](const shared_ptr<Block> &block) {
            auto col = block->col(Customer::CUSTKEY);
            auto size = block->size();
            for (auto i = 0u; i < size; ++i) {
                container.insert(col->next().asInt());
            }
        });

        orderTable->blocks()->foreach([&container](const shared_ptr<Block>& block){
            auto bitmap = make_shared<SimpleBitmap>(block->limit());
            auto col = block->col(Orders::CUSTKEY);
            auto end = container.cend();
            uint32_t size = block->size();
                for (uint32_t i = 0; i < size; ++i) {
                    auto key = col->next().asInt();
                    if (container.find(key)!= end) {
                        bitmap->put(col->pos());
                    }
                }
//            return block->mask(bitmap);
        });

        FilterExecutor::inst->reset();
    }
}


BENCHMARK_F(JoinBenchmark, LQF)(benchmark::State &state) {
    ByteArray segment("HOUSEHOLD");
    for (auto _ : state) {
        //run your benchmark
        auto customerTable = ParquetTable::Open(Customer::path, {Customer::CUSTKEY, Customer::MKTSEGMENT});
        auto orderTable = ParquetTable::Open(Orders::path, {Orders::CUSTKEY});

        ColFilter custFilter(
                {new SboostPredicate<ByteArrayType>(Customer::MKTSEGMENT, bind(&ByteArrayDictEq::build, segment))});
        auto filteredCustTable = custFilter.filter(*customerTable);

        FilterJoin orderOnCustFilterJoin(Orders::CUSTKEY, Customer::CUSTKEY);
        auto joined = orderOnCustFilterJoin.join(*orderTable, *filteredCustTable);
        size_ = joined->size();

        FilterExecutor::inst->reset();
    }
}
