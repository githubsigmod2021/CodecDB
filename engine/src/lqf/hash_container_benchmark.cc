//
// Created by Harper on 6/26/20.
//
#include <benchmark/benchmark.h>
#include "threadpool.h"
#include "hash_container.h"
#include "test_util.h"

using namespace lqf;
using namespace lqf::threadpool;
using namespace lqf::hashcontainer;

class HashContainerBenchmark : public benchmark::Fixture {
protected:
    shared_ptr<Executor> executor = Executor::Make(20);
public:
    HashContainerBenchmark() {}
};

BENCHMARK_F(HashContainerBenchmark, Sparse32)(benchmark::State &state) {
    for (auto _: state) {
        Hash32SparseContainer container(colOffset(4), 300000);

        vector<function<int()>> tasks;
        for (int i = 0; i < 20; ++i) {
            tasks.push_back([i, &container]() {
                for (int j = 0; j < 25000; ++j) {
                    auto key = i * 25000 + j + 100000;
                    DataRow &row = container.add(key);
                    row[0] = j;
                    row[1] = j * 0.1;
                    row[2] = 2 * j;
                    row[3] = 3 * j;
                }
                return 0;
            });
        }
        executor->invokeAll(tasks);

        vector<function<int()>> tasks2;
        for (int i = 0; i < 20; ++i) {
            tasks2.push_back([i, &container]() {
                int count = 0;
                for (int j = 0; j < 35000; ++j) {
                    auto key = i * 35000 + j;
                    DataRow *row = container.get(key);
                    if (row != NULL) {
                        ++count;
                    }
                }
                return count;
            });
        }
        blackhole(executor->invokeAll(tasks2));
    }
}

BENCHMARK_F(HashContainerBenchmark, Sparse64)(benchmark::State &state) {
    for (auto _: state) {
        Hash64SparseContainer container(colOffset(4), 300000);

        vector<function<int()>> tasks;
        for (int i = 0; i < 20; ++i) {
            tasks.push_back([i, &container]() {
                for (int j = 0; j < 25000; ++j) {
                    auto key = i * 25000 + j + 100000;
                    DataRow &row = container.add(key);
                    row[0] = j;
                    row[1] = j * 0.1;
                    row[2] = 2 * j;
                    row[3] = 3 * j;
                }
                return 0;
            });
        }
        executor->invokeAll(tasks);

        vector<function<int()>> tasks2;
        for (int i = 0; i < 20; ++i) {
            tasks2.push_back([i, &container]() {
                int count = 0;
                for (int j = 0; j < 35000; ++j) {
                    auto key = i * 35000 + j;
                    DataRow *row = container.get(key);
                    if (row != NULL) {
                        ++count;
                    }
                }
                return count;
            });
        }
        blackhole(executor->invokeAll(tasks2));
    }
}

BENCHMARK_F(HashContainerBenchmark, Dense32)(benchmark::State &state) {
    for (auto _: state) {
        Hash32DenseContainer container(colOffset(4), 300000);

        vector<function<int()>> tasks;
        for (int i = 0; i < 20; ++i) {
            tasks.push_back([i, &container]() {
                for (int j = 0; j < 25000; ++j) {
                    auto key = i * 25000 + j + 100000;
                    DataRow &row = container.add(key);
                    row[0] = j;
                    row[1] = j * 0.1;
                    row[2] = 2 * j;
                    row[3] = 3 * j;
                }
                return 0;
            });
        }
        executor->invokeAll(tasks);

        vector<function<int()>> tasks2;
        for (int i = 0; i < 20; ++i) {
            tasks2.push_back([i, &container]() {
                int count = 0;
                for (int j = 0; j < 35000; ++j) {
                    auto key = i * 35000 + j;
                    DataRow *row = container.get(key);
                    if (row != NULL) {
                        ++count;
                    }
                }
                return count;
            });
        }
        blackhole(executor->invokeAll(tasks2));
    }
}

BENCHMARK_F(HashContainerBenchmark, Dense64)(benchmark::State &state) {
    for (auto _: state) {
        Hash64DenseContainer container(colOffset(4), 300000);

        vector<function<int()>> tasks;
        for (int i = 0; i < 20; ++i) {
            tasks.push_back([i, &container]() {
                for (int j = 0; j < 25000; ++j) {
                    auto key = i * 25000 + j + 100000;
                    DataRow &row = container.add(key);
                    row[0] = j;
                    row[1] = j * 0.1;
                    row[2] = 2 * j;
                    row[3] = 3 * j;
                }
                return 0;
            });
        }
        executor->invokeAll(tasks);

        vector<function<int()>> tasks2;
        for (int i = 0; i < 20; ++i) {
            tasks2.push_back([i, &container]() {
                int count = 0;
                for (int j = 0; j < 35000; ++j) {
                    auto key = i * 35000 + j;
                    DataRow *row = container.get(key);
                    if (row != NULL) {
                        ++count;
                    }
                }
                return count;
            });
        }
        blackhole(executor->invokeAll(tasks2));
    }
}
