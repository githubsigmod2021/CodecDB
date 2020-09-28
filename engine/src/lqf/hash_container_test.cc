//
// Created by Harper on 6/26/20.
//

#include <gtest/gtest.h>
#include "hash_container.h"
#include "threadpool.h"

using namespace lqf;
using namespace lqf::threadpool;
using namespace lqf::hashcontainer;

TEST(Hash32SparseContainerTest, Access) {
    auto executor = Executor::Make(20);

    Hash32SparseContainer container(lqf::colOffset(3));

    vector<function<int()>> tasks;

    for (int i = 0; i < 10; ++i) {
        tasks.push_back([&container, i]() {
            for (int j = 0; j < 40000; ++j) {
                DataRow &row = container.add(i * 40000 + j);
                row[0] = j;
                row[1] = j * 0.1;
                row[2] = 2 * j;
            }
            return 0;
        });
    }

    executor->invokeAll(tasks);

    EXPECT_EQ(container.size(), 400000);

    vector<function<int()>> tasks2;
    for (int i = 0; i < 10; ++i) {
        tasks2.push_back([&container, i]() {
            for (int j = 0; j < 40000; ++j) {
                DataRow *row = container.get(i * 40000 + j);
                EXPECT_EQ((*row)[0].asInt(), j);
                EXPECT_EQ((*row)[1].asDouble(), j * 0.1);
                EXPECT_EQ((*row)[2].asInt(), 2 * j);
                DataRow *nerow = container.get(400000 + j);
                EXPECT_TRUE(nerow == nullptr);
            }
            return 0;
        });
    }
    executor->invokeAll(tasks2);

    unordered_set<int> set;
    int max = INT32_MIN;
    int min = INT32_MAX;
    auto ite = container.iterator();
    while (ite->hasNext()) {
        auto &next = ite->next();
        auto key = next.first;
        max = std::max(max, key);
        min = std::min(min, key);
        set.insert(key);
        DataRow &value = next.second;
        auto localkey = key % 40000;
        EXPECT_EQ(value[0].asInt(), localkey);
        EXPECT_EQ(value[1].asDouble(), localkey * 0.1);
        EXPECT_EQ(value[2].asInt(), localkey * 2);
    }
    EXPECT_EQ(0, min);
    EXPECT_EQ(399999, max);
    EXPECT_EQ(400000, set.size());
}

TEST(Hash64SparseContainerTest, Access) {
    auto executor = Executor::Make(20);

    Hash64SparseContainer container(lqf::colOffset(3));

    vector<function<int()>> tasks;

    for (int i = 0; i < 10; ++i) {
        tasks.push_back([&container, i]() {
            for (int j = 0; j < 40000; ++j) {
                DataRow &row = container.add(i * 40000 + j);
                row[0] = j;
                row[1] = j * 0.1;
                row[2] = 2 * j;
            }
            return 0;
        });
    }

    executor->invokeAll(tasks);

    EXPECT_EQ(container.size(), 400000);

    vector<function<int()>> tasks2;
    for (int i = 0; i < 10; ++i) {
        tasks2.push_back([&container, i]() {
            for (int j = 0; j < 40000; ++j) {
                DataRow *row = container.get(i * 40000 + j);
                EXPECT_EQ((*row)[0].asInt(), j);
                EXPECT_EQ((*row)[1].asDouble(), j * 0.1);
                EXPECT_EQ((*row)[2].asInt(), 2 * j);
                DataRow *nerow = container.get(400000 + j);
                EXPECT_TRUE(nerow == nullptr);
            }
            return 0;
        });
    }
    executor->invokeAll(tasks2);

    unordered_set<int64_t> set;
    int64_t max = INT64_MIN;
    int64_t min = INT64_MAX;
    auto ite = container.iterator();
    while (ite->hasNext()) {
        auto &next = ite->next();
        auto key = next.first;
        max = std::max(max, key);
        min = std::min(min, key);
        set.insert(key);
        DataRow &value = next.second;
        auto localkey = key % 40000;
        EXPECT_EQ(value[0].asInt(), localkey);
        EXPECT_EQ(value[1].asDouble(), localkey * 0.1);
        EXPECT_EQ(value[2].asInt(), localkey * 2);
    }
    EXPECT_EQ(0, min);
    EXPECT_EQ(399999, max);
    EXPECT_EQ(400000, set.size());
}

TEST(Hash32DenseContainerTest, Access) {
    auto executor = Executor::Make(20);

    Hash32DenseContainer container(lqf::colOffset(3));

    vector<function<int()>> tasks;

    for (int i = 0; i < 10; ++i) {
        tasks.push_back([&container, i]() {
            for (int j = 0; j < 40000; ++j) {
                DataRow &row = container.add(i * 40000 + j);
                row[0] = j;
                row[1] = j * 0.1;
                row[2] = 2 * j;
            }
            return 0;
        });
    }

    executor->invokeAll(tasks);

    EXPECT_EQ(container.size(), 400000);

    vector<function<int()>> tasks2;
    for (int i = 0; i < 10; ++i) {
        tasks2.push_back([&container, i]() {
            for (int j = 0; j < 40000; ++j) {
                DataRow *row = container.get(i * 40000 + j);
                EXPECT_TRUE(row != nullptr);
                if (row != nullptr) {
                    EXPECT_EQ((*row)[0].asInt(), j);
                    EXPECT_EQ((*row)[1].asDouble(), j * 0.1);
                    EXPECT_EQ((*row)[2].asInt(), 2 * j);
                }
                DataRow *nerow = container.get(400000 + j);
                EXPECT_TRUE(nerow == nullptr);
            }
            return 0;
        });
    }
    executor->invokeAll(tasks2);

    unordered_set<int> set;
    int max = INT32_MIN;
    int min = INT32_MAX;
    auto ite = container.iterator();
    while (ite->hasNext()) {
        auto &next = ite->next();
        auto key = next.first;
        max = std::max(max, key);
        min = std::min(min, key);
        set.insert(key);
        DataRow &value = next.second;
        auto localkey = key % 40000;
        EXPECT_EQ(value[0].asInt(), localkey);
        EXPECT_EQ(value[1].asDouble(), localkey * 0.1);
        EXPECT_EQ(value[2].asInt(), localkey * 2);
    }
    EXPECT_EQ(0, min);
    EXPECT_EQ(399999, max);
    EXPECT_EQ(400000, set.size());
}

TEST(Hash64DenseContainerTest, Access) {
    auto executor = Executor::Make(20);

    Hash64DenseContainer container(lqf::colOffset(3));

    vector<function<int()>> tasks;

    for (int i = 0; i < 10; ++i) {
        tasks.push_back([&container, i]() {
            for (int j = 0; j < 40000; ++j) {
                DataRow &row = container.add(i * 40000 + j);
                row[0] = j;
                row[1] = j * 0.1;
                row[2] = 2 * j;
            }
            return 0;
        });
    }

    executor->invokeAll(tasks);

    EXPECT_EQ(container.size(), 400000);

    vector<function<int()>> tasks2;
    for (int i = 0; i < 10; ++i) {
        tasks2.push_back([&container, i]() {
            for (int j = 0; j < 40000; ++j) {
                DataRow *row = container.get(i * 40000 + j);
                EXPECT_EQ((*row)[0].asInt(), j);
                EXPECT_EQ((*row)[1].asDouble(), j * 0.1);
                EXPECT_EQ((*row)[2].asInt(), 2 * j);
                DataRow *nerow = container.get(400000 + j);
                EXPECT_TRUE(nerow == nullptr);
            }
            return 0;
        });
    }
    executor->invokeAll(tasks2);

    unordered_set<int64_t> set;
    int64_t max = INT64_MIN;
    int64_t min = INT64_MAX;
    auto ite = container.iterator();
    while (ite->hasNext()) {
        auto &next = ite->next();
        auto key = next.first;
        max = std::max(max, key);
        min = std::min(min, key);
        set.insert(key);
        DataRow &value = next.second;
        auto localkey = key % 40000;
        EXPECT_EQ(value[0].asInt(), localkey);
        EXPECT_EQ(value[1].asDouble(), localkey * 0.1);
        EXPECT_EQ(value[2].asInt(), localkey * 2);
    }
    EXPECT_EQ(0, min);
    EXPECT_EQ(399999, max);
    EXPECT_EQ(400000, set.size());
}
