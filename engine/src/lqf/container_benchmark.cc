//
// Created by Harper on 4/21/20.
//

#include <benchmark/benchmark.h>
#include <unordered_set>
#include <unordered_map>
#include "container.h"
#include "bitmap.h"
#include "threadpool.h"

using namespace lqf;
using namespace lqf::container;
using namespace lqf::threadpool;

class SetWriteBenchmark : public benchmark::Fixture {
protected:
    shared_ptr<Executor> executor;

    vector<int32_t> values;

    int32_t total;

    int32_t LIMIT = 1000000;
public:

    SetWriteBenchmark() {
        executor = Executor::Make(25);
        srand(time(NULL));

        values.clear();
        for (int i = 0; i < LIMIT; ++i) {
            values.push_back(rand() % LIMIT);
        }
    }

    virtual ~SetWriteBenchmark() {
    }
};


BENCHMARK_F(SetWriteBenchmark, Int32)(benchmark::State &state) {
    for (auto _ : state) {
        //run your benchmark
        auto chs32_ = new PhaseConcurrentHashSet<Int32>(LIMIT);
        vector<function<int()>> tasks;
        function<int(int)> task = [this, chs32_](int input) {
            for (int i = 0; i < LIMIT / 10; ++i) {
                chs32_->add(values[input * (LIMIT / 10) + i]);
            }
            return input;
        };
        for (int i = 0; i < 10; ++i) {
            tasks.push_back(bind(task, i));
        }
        executor->invokeAll(tasks);
        total = chs32_->size();
        delete chs32_;
    }
}

BENCHMARK_F(SetWriteBenchmark, Int64)(benchmark::State &state) {
    for (auto _ : state) {
        auto chs64_ = new PhaseConcurrentHashSet<Int64>();
        vector<function<int()>> tasks;
        function<int(int)> task = [chs64_, this](int input) {
            for (int i = 0; i < LIMIT / 10; ++i) {
                chs64_->add(values[input * (LIMIT / 10) + i]);
            }
            return input;
        };
        for (int i = 0; i < 10; ++i) {
            tasks.push_back(bind(task, i));
        }
        executor->invokeAll(tasks);
        total = chs64_->size();
        delete chs64_;
    }
}

BENCHMARK_F(SetWriteBenchmark, Int32Bitmap)(benchmark::State &state) {
    for (auto _ : state) {
        auto chs64_ = new ConcurrentBitmap(LIMIT);
        vector<function<int()>> tasks;
        function<int(int)> task = [chs64_, this](int input) {
            for (int i = 0; i < LIMIT / 10; ++i) {
                chs64_->put(values[input * (LIMIT / 10) + i]);
            }
            return input;
        };
        for (int i = 0; i < 10; ++i) {
            tasks.push_back(bind(task, i));
        }
        executor->invokeAll(tasks);
        total = chs64_->size();
        delete chs64_;
    }
}

BENCHMARK_F(SetWriteBenchmark, Int32SingleThread)(benchmark::State &state) {
    for (auto _ : state) {
        auto chs32_ = new PhaseConcurrentHashSet<Int32>();
        for (auto &val: values) {
            chs32_->add(val);
        }
        total = chs32_->size();
        delete chs32_;
    }
}

BENCHMARK_F(SetWriteBenchmark, Int64SingleThread)(benchmark::State &state) {
    for (auto _ : state) {
        auto chs64_ = new PhaseConcurrentHashSet<Int64>();
        for (auto &val: values) {
            chs64_->add(val);
        }
        total = chs64_->size();
        delete chs64_;
    }
}

BENCHMARK_F(SetWriteBenchmark, StlInt32)(benchmark::State &state) {
    for (auto _ : state) {
        unordered_set<int32_t> set;
        for (auto &val: values) {
            set.insert(val);
        }
        total = set.size();
    }
}

BENCHMARK_F(SetWriteBenchmark, StlInt64)(benchmark::State &state) {
    for (auto _ : state) {
        unordered_set<int64_t> set;
        for (auto &val: values) {
            set.insert(val);
        }
        total = set.size();
    }
}

class SetReadBenchmark : public benchmark::Fixture {
protected:
    shared_ptr<Executor> executor;

    vector<int32_t> values;

    PhaseConcurrentHashSet<Int32> chs32_;
    PhaseConcurrentHashSet<Int64> chs64_;

    unordered_set<int32_t> us32_;
    unordered_set<int64_t> us64_;

    int32_t total;
public:

    SetReadBenchmark() {
        executor = Executor::Make(25);
        srand(time(NULL));

        for (int i = 0; i < 800000; ++i) {
            int value = rand();
            values.push_back(value);
            chs32_.add(value);
            chs64_.add(value);
            us32_.insert(value);
            us64_.insert(value);
        }
    }

    virtual ~SetReadBenchmark() {
    }
};

BENCHMARK_F(SetReadBenchmark, Int32)(benchmark::State &state) {
    for (auto _:state) {
        for (int i = 0; i < 5000; ++i) {
            uint32_t key = ((uint32_t) rand()) % values.size();
            total = chs32_.test(values[key]);
        }
        for (int i = 0; i < 5000; ++i) {
            total = chs32_.test(rand());
        }
    }
}

BENCHMARK_F(SetReadBenchmark, Int64)(benchmark::State &state) {
    for (auto _:state) {
        for (int i = 0; i < 5000; ++i) {
            uint32_t key = ((uint32_t) rand()) % values.size();
            total = chs64_.test(values[key]);
        }
        for (int i = 0; i < 5000; ++i) {
            total = chs64_.test(rand());
        }
    }
}

BENCHMARK_F(SetReadBenchmark, StlInt32)(benchmark::State &state) {
    for (auto _:state) {
        for (int i = 0; i < 5000; ++i) {
            uint32_t key = ((uint32_t) rand()) % values.size();
            total = us32_.find(values[key]) != us32_.end();
        }
        for (int i = 0; i < 5000; ++i) {
            total = us32_.find(rand()) != us32_.end();
        }
    }
}

BENCHMARK_F(SetReadBenchmark, StlInt64)(benchmark::State &state) {
    for (auto _:state) {
        for (int i = 0; i < 5000; ++i) {
            uint32_t key = ((uint32_t) rand()) % values.size();
            total = us64_.find(values[key]) != us64_.end();
        }
        for (int i = 0; i < 5000; ++i) {
            total = us64_.find(rand()) != us64_.end();
        }
    }
}

struct DemoObject {
    int32_t value1_;
    int32_t value2_;
};

class MapWriteBenchmark : public benchmark::Fixture {
public:
    shared_ptr<Executor> executor;
    uint32_t limit = 700000;
    uint32_t split = 10;
    uint32_t splice = limit / split;
    int32_t blackhole;

    MapWriteBenchmark() {
        executor = Executor::Make(10);
        srand(time(NULL));
    }

    virtual ~MapWriteBenchmark() {
    }
};

int run_task(MapWriteBenchmark *benchmark, PhaseConcurrentHashMap<Int32, DemoObject *> *map, int input) {
    for (uint32_t i = 0; i < benchmark->splice; ++i) {
        auto index = input * benchmark->splice + i;
        map->put(index, new DemoObject{0, 0});
    }
    return input;
}

BENCHMARK_F(MapWriteBenchmark, Int32)(benchmark::State &state) {
    for (auto _ : state) {
        //run your benchmark
        auto chs32_ = new PhaseConcurrentHashMap<Int32, DemoObject *>(limit);
        vector<function<int()>> tasks;

        for (uint32_t i = 0; i < split; ++i) {
            tasks.push_back(bind(run_task, this, chs32_, i));
        }
        executor->invokeAll(tasks);
        blackhole = chs32_->size();
        delete chs32_;
    }
}

BENCHMARK_F(MapWriteBenchmark, Int64)(benchmark::State &state) {
    for (auto _ : state) {
        auto chs64_ = new PhaseConcurrentHashMap<Int64, DemoObject *>(limit);
        vector<function<int()>> tasks;
        function<int(int)> task = [this, chs64_](int input) {
            for (uint32_t i = 0; i < splice; ++i) {
                auto index = input * splice + i;
                chs64_->put(index, new DemoObject{0, 0});
            }
            return input;
        };
        for (uint32_t i = 0; i < split; ++i) {
            tasks.push_back(bind(task, i));
        }
        executor->invokeAll(tasks);
        blackhole = chs64_->size();
        delete chs64_;
    }
}

BENCHMARK_F(MapWriteBenchmark, StlInt32)(benchmark::State &state) {
    for (auto _ : state) {
        unordered_map<int32_t, DemoObject *> map;
        for (uint32_t i = 0; i < limit; ++i) {
            map[i] = new DemoObject{0, 0};
        }
        blackhole = map.size();
    }
}

BENCHMARK_F(MapWriteBenchmark, StlInt64)(benchmark::State &state) {
    for (auto _ : state) {
        unordered_map<int64_t, DemoObject *> map;
        for (uint32_t i = 0; i < limit; ++i) {
            map[i] = new DemoObject{0, 0};
        }
        blackhole = map.size();
    }
}

class MapReadBenchmark : public benchmark::Fixture {
protected:
    shared_ptr<Executor> executor;

    vector<int32_t> keys;

    PhaseConcurrentHashMap<Int32, DemoObject *> chs32_;
    PhaseConcurrentHashMap<Int64, DemoObject *> chs64_;

    unordered_map<int32_t, DemoObject *> us32_;
    unordered_map<int64_t, DemoObject *> us64_;

    int32_t total;
public:

    MapReadBenchmark() {
        executor = Executor::Make(25);
        srand(time(NULL));

        for (int i = 0; i < 800000; ++i) {
            int key = rand();
            keys.push_back(key);

            DemoObject *value = new DemoObject{rand(), rand()};

            chs32_.put(key, value);
            chs64_.put(key, value);
            us32_[key] = value;
            us64_[key] = value;
        }
    }

    virtual ~MapReadBenchmark() {
    }
};

BENCHMARK_F(MapReadBenchmark, Int32)(benchmark::State &state) {
    for (auto _:state) {
        for (int i = 0; i < 5000; ++i) {
            chs32_.get(rand() % 800000);
        }
        for (int i = 0; i < 5000; ++i) {
            chs32_.get(rand());
        }
    }
}

BENCHMARK_F(MapReadBenchmark, Int64)(benchmark::State &state) {
    for (auto _:state) {
        for (int i = 0; i < 5000; ++i) {
            chs64_.get(rand() % 800000);
        }
        for (int i = 0; i < 5000; ++i) {
            chs64_.get(rand());
        }
    }
}

BENCHMARK_F(MapReadBenchmark, StlInt32)(benchmark::State &state) {
    for (auto _:state) {
        for (int i = 0; i < 5000; ++i) {
            us32_.find(rand() % 800000);
        }
        for (int i = 0; i < 5000; ++i) {
            us32_.find(rand());
        }
    }
}

BENCHMARK_F(MapReadBenchmark, StlInt64)(benchmark::State &state) {
    for (auto _:state) {
        for (int i = 0; i < 5000; ++i) {
            us64_.find(rand() % 800000);
        }
        for (int i = 0; i < 5000; ++i) {
            us64_.find(rand());
        }
    }
}