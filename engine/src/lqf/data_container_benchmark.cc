//
// Created by Harper on 6/22/20.
//

#include <benchmark/benchmark.h>
#include "threadpool.h"
#include "data_container.h"
#include "container.h"
#include "data_model.h"

// Compare the performance between ConcurrentMemRowMap and PhaseConcurrentHashMap<int, MemDataRow>

using namespace lqf;
using namespace lqf::threadpool;
using namespace lqf::datacontainer;
using namespace lqf::container;

class DataContainerBenchmark : public benchmark::Fixture {
protected:
    shared_ptr<Executor> executor;

public:

    DataContainerBenchmark() {
        executor = Executor::Make(20);
        srand(time(NULL));

    }

    virtual ~DataContainerBenchmark() {
    }
};

BENCHMARK_F(DataContainerBenchmark, CMemRowMapWrite)(benchmark::State &state) {
    for (auto _:state) {
        CInt32MemRowMap map(1048576, colOffset(4));

        vector<function<int()>> tasks;
        for (int i = 0; i < 20; ++i) {
            tasks.push_back([i, &map]() {
                for (int k = 0; k < 50000; ++k) {
                    DataRow &row = map.insert(k);
                    row[0] = k;
                    row[1] = k * 0.1;
                    row[2] = 2 * k;
                    row[3] = 3 * k + 0.1;
                }
                return 0;
            });
        }
        executor->invokeAll(tasks);
    }
}

BENCHMARK_F(DataContainerBenchmark, PCHashMapWrite)(benchmark::State &state) {
    for (auto _: state) {
        PhaseConcurrentHashMap<Int32, MemDataRow *> map(1048576);
        vector<uint32_t> offset = colOffset(4);
        vector<function<int()>> tasks;
        for (int i = 0; i < 20; ++i) {
            tasks.push_back([i, &map, &offset]() {
                for (int k = 0; k < 50000; ++k) {
                    MemDataRow *row = new MemDataRow(offset);
                    (*row)[0] = k;
                    (*row)[1] = k * 0.1;
                    (*row)[2] = 2 * k;
                    (*row)[3] = 3 * k + 0.1;
                    map.put(k, row);
                }
                return 0;
            });
        }
        executor->invokeAll(tasks);
    }
}

BENCHMARK_F(DataContainerBenchmark, CMemRowMapRead)(benchmark::State &state) {
    for (auto _ : state) {
        CInt64MemRowMap map(1048576, colOffset(4));

        vector<function<int()>> tasks;
        for (int i = 0; i < 20; ++i) {
            tasks.push_back([i, &map]() {
                for (int k = 0; k < 50000; ++k) {
                    DataRow &row = map.insert(k);
                    row[0] = k;
                    row[1] = k * 0.1;
                    row[2] = 2 * k;
                    row[3] = 3 * k + 0.1;
                }
                return 0;
            });
        }
        executor->invokeAll(tasks);

        vector<function<int()>> readtasks;
        for (int i = 0; i < 20; ++i) {
            readtasks.push_back([i, &map]() {
                for (int k = 0; i < 50000; ++k) {
                    map.find(k);
                }
                return 0;
            });
        }
        executor->invokeAll(tasks);
    }
}

BENCHMARK_F(DataContainerBenchmark, PCHashMapRead)(benchmark::State &state) {
    for (auto _ : state) {
        PhaseConcurrentHashMap<Int32, MemDataRow *> map(1048576);
        vector<uint32_t> offset = colOffset(4);
        vector<function<int()>> tasks;
        for (int i = 0; i < 20; ++i) {
            tasks.push_back([i, &map, &offset]() {
                for (int k = 0; k < 50000; ++k) {
                    MemDataRow *row = new MemDataRow(offset);
                    (*row)[0] = k;
                    (*row)[1] = k * 0.1;
                    (*row)[2] = 2 * k;
                    (*row)[3] = 3 * k + 0.1;
                    map.put(k, row);
                }
                return 0;
            });
        }
        executor->invokeAll(tasks);

        vector<function<int()>> readtasks;
        for (int i = 0; i < 20; ++i) {
            readtasks.push_back([i, &map]() {
                for (int k = 0; i < 50000; ++k) {
                    map.get(k);
                }
                return 0;
            });
        }
        executor->invokeAll(tasks);
    }
}
