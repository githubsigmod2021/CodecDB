//
// Created by harper on 3/14/20.
//
#include <gtest/gtest.h>
#include "threadpool.h"

using namespace lqf::threadpool;

TEST(ExecutorTest, Shutdown) {
    auto executor = Executor::Make(10);
    executor->shutdown();
}

TEST(ExecutorTest, Submit) {
    auto executor = Executor::Make(10);

    int *i = new int(0);
    executor->submit([=]() {
        *i += 5;
    });
    usleep(100000);

    EXPECT_EQ(5, *i);
    executor->shutdown();
    delete i;
}

TEST(ExecutorTest, InvokeAll) {

    auto executor = Executor::Make(10);

    vector<function<int32_t()>> tasks;
    for (int i = 0; i < 20; i++) {
        tasks.push_back([]() {
            usleep(1200000);
            auto i = time(NULL) & 0xFFFFFFFF;
            return i;
        });
    }

    unique_ptr<vector<int32_t>> result = executor->invokeAll(tasks);
    executor->shutdown();

    for (int i = 0; i < 10; i++) {
        auto start = (*result)[i];
        auto end = (*result)[i + 10];
        EXPECT_TRUE(start >= end - 2);
    }
}