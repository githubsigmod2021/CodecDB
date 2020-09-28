//
// Created by Harper on 4/21/20.
//

#include <benchmark/benchmark.h>
#include <unordered_set>
#include <unordered_map>
#include <functional>
#include "container.h"
#include "bitmap.h"
#include "data_model.h"
#include "rowcopy.h"
#include "test_util.h"

using namespace lqf;
using namespace lqf::container;
using namespace lqf::threadpool;
using namespace lqf::rowcopy;


class RowCopyBenchmark : public benchmark::Fixture {
protected:
public:

    shared_ptr<MemBlock> mem_source_;
    shared_ptr<ParquetBlock> parquet_source_;
    unique_ptr<function<void(DataRow &, DataRow &)>> copier1_;
    unique_ptr<function<void(DataRow &, DataRow &)>> copier2_;

    RowCopyBenchmark() {
        mem_source_ = make_shared<MemBlock>(300000, 8);

        rowcopy::RowCopyFactory f;
        copier1_ = f.from(RAW)->to(RAW)
                ->from_layout(vector<uint32_t>{0, 1, 2, 3, 4, 5, 6, 7, 8})
                ->to_layout(vector<uint32_t>{0, 1, 2, 3, 4, 5, 6, 7, 8})
                ->field(F_REGULAR, 0, 0)
                ->field(F_REGULAR, 1, 1)
                ->field(F_REGULAR, 2, 2)
                ->field(F_REGULAR, 3, 3)
                ->field(F_REGULAR, 4, 4)
                ->field(F_REGULAR, 5, 5)
                ->field(F_REGULAR, 6, 6)
                ->field(F_REGULAR, 7, 7)
                ->build();
        rowcopy::RowCopyFactory f2;
        copier2_ = f2.from(EXTERNAL)->to(RAW)
                ->field(F_REGULAR, 0, 0)
                ->field(F_REGULAR, 1, 1)
                ->field(F_REGULAR, 2, 2)
                ->field(F_REGULAR, 3, 3)
                ->field(F_REGULAR, 4, 4)
                ->field(F_REGULAR, 5, 5)
                ->field(F_REGULAR, 6, 6)
                ->field(F_REGULAR, 7, 7)
                ->build();
    }

    virtual ~RowCopyBenchmark() {
    }


};


BENCHMARK_F(RowCopyBenchmark, CopyVirtualFromMem)(benchmark::State &state) {
    for (auto _ : state) {
        auto mem_dest = make_shared<MemBlock>(mem_source_->size(), 8);
        auto reader = mem_source_->rows();
        auto writer = mem_dest->rows();
        auto size = mem_source_->size();
        for (uint32_t i = 0; i < size; ++i) {
            DataRow& next = reader->next();
            if (i % 3 == 0)
                (*writer)[i] = next;
        }
        blackhole(mem_dest);
    }
}

BENCHMARK_F(RowCopyBenchmark, CopyDirectFromMem)(benchmark::State &state) {
    for (auto _ : state) {
        auto mem_dest = make_shared<MemBlock>(mem_source_->size(), 8);
        auto reader = mem_source_->rows();
        auto writer = mem_dest->rows();
        auto size = mem_source_->size();
        for (uint32_t i = 0; i < size; ++i) {
            DataRow& next = reader->next();
            if (i % 3 == 0)
                elements::rc_memcpy((*writer)[i], next);
        }
        blackhole(mem_dest);
    }
}

BENCHMARK_F(RowCopyBenchmark, CopyMethodFromMem)(benchmark::State &state) {
    for (auto _ : state) {
        auto mem_dest = make_shared<MemBlock>(mem_source_->size(), 8);
        auto reader = mem_source_->rows();
        auto writer = mem_dest->rows();
        auto size = mem_source_->size();

        for (uint32_t i = 0; i < size; ++i) {
            DataRow& next = reader->next();
            if (i % 3 == 0)
                (*copier1_)((*writer)[i], next);
        }
        blackhole(mem_dest);
    }
}

BENCHMARK_F(RowCopyBenchmark, CopyVirtualFromParquet)(benchmark::State &state) {
    for (auto _ : state) {
        auto table = ParquetTable::Open("testres/lineitem2", (1 << 10) - 1);
        auto blocks = table->blocks()->collect();
        parquet_source_ = dynamic_pointer_cast<ParquetBlock>((*blocks)[0]);

        auto reader = parquet_source_->rows();
        auto size = parquet_source_->size();

        auto mem_dest = make_shared<MemBlock>(size, 8);
        auto writer = mem_dest->rows();
        for (uint32_t i = 0; i < size; ++i) {
            (*writer)[i] = reader->next();
        }
        blackhole(mem_dest);
    }
}

using namespace std;

BENCHMARK_F(RowCopyBenchmark, CopyMethodFromParquet)(benchmark::State &state) {
    for (auto _ : state) {
        auto table = ParquetTable::Open("testres/lineitem2", (1 << 10) - 1);
        auto blocks = table->blocks()->collect();
        parquet_source_ = dynamic_pointer_cast<ParquetBlock>((*blocks)[0]);

        auto reader = parquet_source_->rows();
        auto size = parquet_source_->size();

        auto mem_dest = make_shared<MemBlock>(size, 8);
        auto writer = mem_dest->rows();

        for (uint32_t i = 0; i < size; ++i) {
            (*copier2_)((*writer)[i], reader->next());
        }
        blackhole(mem_dest);
    }
}