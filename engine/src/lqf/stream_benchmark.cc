//
// Created by Harper on 5/7/20.
//

#include <benchmark/benchmark.h>
#include "stream.h"

using namespace lqf;

class StreamBenchmark : public benchmark::Fixture {
public:
    uint64_t sum;

    StreamBenchmark() {

    }
};

uint32_t __attribute__ ((noinline)) hash_func(const int32_t &input) {
    auto result = input;
    for (int i = 0; i < 10; ++i) {
        result += ((((result * 0x2422adff) << 21) + result * 0x12342342) << 12) + input * 0x234322434;
    }
    return result;
}

uint32_t __attribute__ ((noinline)) hash2_func(const uint32_t &input) {
    auto result = input;
    for (int i = 0; i < 10; ++i) {
        result = ((result * 0x2422bbae) << 21) + input * 0x12342ddef;
    }
    return result;
}

BENCHMARK_F(StreamBenchmark, RunPlain2Layer)(benchmark::State &state) {
    sum = 0;
    for (auto _: state) {
        for (uint32_t i = 1; i < 150000; ++i) {
            sum += hash2_func(hash_func(i));
        }
    }
}

BENCHMARK_F(StreamBenchmark, RunStream2Layer)(benchmark::State &state) {
    sum = 0;
    for (auto _: state) {
        auto stream = IntStream::Make(1, 150000);
        function<uint32_t(const int32_t &)> hasher = hash_func;
        function<uint32_t(const uint32_t &)> hasher2 = hash2_func;
        auto hashed = stream->map(hasher)->map(hasher2);
        hashed->foreach([=](int value) {
            sum += value;
        });
    }
}

