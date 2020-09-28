//
// Created by harper on 2/27/20.
//
#include <cstdlib>
#include <gtest/gtest.h>
#include "sort.h"

using namespace lqf;

TEST(TopNTest, Sort) {
    srand(time(NULL));

    auto table = MemTable::Make(4);
    auto block1 = table->allocate(100);
    auto block2 = table->allocate(200);
    auto block3 = table->allocate(300);
    auto block4 = table->allocate(400);

    vector<int> buffer;

    auto rows1 = block1->rows();
    for (int i = 0; i < 100; i++) {
        int next = rand();
        buffer.push_back(next);
        (*rows1)[i][0] = next;
    }
    auto rows2 = block2->rows();
    for (int i = 0; i < 200; i++) {
        int next = rand();
        buffer.push_back(next);
        (*rows2)[i][0] = next;
    }
    auto rows3 = block3->rows();
    for (int i = 0; i < 300; i++) {
        int next = rand();
        buffer.push_back(next);
        (*rows3)[i][0] = next;
    }
    auto rows4 = block4->rows();
    for (int i = 0; i < 400; i++) {
        int next = rand();
        buffer.push_back(next);
        (*rows4)[i][0] = next;
    }

    TopN top10(10, [](DataRow *a, DataRow *b) {
        return (*a)[0].asInt() < (*b)[0].asInt();
    });

    auto sorted = top10.sort(*table);
    auto sortedblock = (*sorted->blocks()->collect())[0];
    EXPECT_EQ(10, sortedblock->size());

    std::sort(buffer.begin(), buffer.end());
    auto sortedrows = sortedblock->rows();
    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(buffer[i], (*sortedrows)[i][0].asInt());
    }
}

TEST(SmallSortTest, Sort) {
    srand(time(NULL));

    auto table = MemTable::Make(4);
    auto block = table->allocate(100);
    auto block2 = table->allocate(200);
    auto block3 = table->allocate(300);
    auto block4 = table->allocate(400);

    vector<int> buffer;

    auto rows = block->rows();
    for (int i = 0; i < 100; i++) {
//        int next = rand();
        int next = i;
        buffer.push_back(next);
        (*rows)[i][2] = next;
    }

    auto rows2 = block2->rows();
    for (int i = 0; i < 200; i++) {
//        int next = rand();
        int next = -500 + i;
        buffer.push_back(next);
        (*rows2)[i][2] = next;
    }

    auto rows3 = block3->rows();
    for (int i = 0; i < 300; i++) {
//        int next = rand();
        int next = -700 + i;
        buffer.push_back(next);
        (*rows3)[i][2] = next;
    }

    auto rows4 = block4->rows();
    for (int i = 0; i < 400; i++) {
//        int next = rand();
        int next = -1200 + i;
        buffer.push_back(next);
        (*rows4)[i][2] = next;
    }
    function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
        return (*a)[2].asInt() < (*b)[2].asInt();
    };

    SmallSort sort(comparator);

    auto sorted = sort.sort(*table);
    auto sortedblock = (*sorted->blocks()->collect())[0];
    EXPECT_EQ(1000, sortedblock->size());

    std::sort(buffer.begin(), buffer.end());

    auto sortedrows = sortedblock->rows();

    for (int i = 0; i < 1000; ++i) {
        EXPECT_EQ(buffer[i], (*sortedrows)[i][2].asInt()) << i;
    }
}