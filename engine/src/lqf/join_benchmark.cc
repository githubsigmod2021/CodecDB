//
// Created by Harper on 4/26/20.
//

#include <benchmark/benchmark.h>
#include "container.h"
#include "bitmap.h"


using namespace lqf;
using namespace lqf::container;

struct DemoObject {
    int value;
};

int bitmap_max_ = 1000000;
int hashmap_size_ = 300000;
int bitmap_card_ = 50000;
int percent = hashmap_size_ / bitmap_card_;

class NotExistJoinBenchmark : public benchmark::Fixture {
protected:
    PhaseConcurrentHashMap<Int32, DemoObject *> hashmap_;
    SimpleBitmap ne_bitmap_;
public:
    NotExistJoinBenchmark() : ne_bitmap_(bitmap_max_) {
        srand(time(NULL));
        for (int i = 0; i < hashmap_size_; ++i) {
            auto key = rand() % bitmap_max_;
            hashmap_.put(key, new DemoObject{key});
            if (i % percent) {
                ne_bitmap_.put(key);
            }
        }
    }
};
//READ HASHMAP
//WRITE BITMAP
//XOR BITMAP
//ITERATE BITMAP

BENCHMARK_F(NotExistJoinBenchmark, WithXor)(benchmark::State &state) {
    for (auto _:state) {
        auto full_bitmap = make_shared<SimpleBitmap>(ne_bitmap_.size());
        auto ite = hashmap_.iterator();
        while (ite->hasNext()) {
            full_bitmap->put(ite->next().first);
        }
        auto xored = *full_bitmap ^ne_bitmap_;
        auto xorite = xored->iterator();
        uint64_t sum = 0;
        while (xorite->hasNext()) {
            auto item = hashmap_.get(xorite->next());
            sum += item->value;
        }
    }
}

//READ HASHMAP
//QUERY BITMAP

BENCHMARK_F(NotExistJoinBenchmark, DirectQuery)(benchmark::State &state) {
    for (auto _ : state) {
        uint64_t sum = 0;
        auto ite = hashmap_.iterator();
        while (ite->hasNext()) {
            auto entry = ite->next();
            if (!ne_bitmap_.check(entry.first)) {
                sum += entry.second->value;
            }
        }
    }
}