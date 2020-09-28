//
// Created by harper on 2/24/20.
//

#include <gtest/gtest.h>
#include "agg.h"
#include "rowcopy.h"

using namespace std;
using namespace lqf;
using namespace lqf::agg;
using namespace lqf::rowcopy;
using namespace lqf::agg::recording;

TEST(AggFieldTest, Max) {

    vector<uint32_t> offset({0, 1, 2});

    MemDataRow storage(offset);

    MemDataRow row(offset);

    IntMax max1(0);
    max1.write_at(0);
    max1.attach(storage);

    for (int i = 0; i < 50; ++i) {
        row[0] = i % 20;
        max1.reduce(row);
    }
    EXPECT_EQ(19, storage[0].asInt());

    IntMax max2(0);
    max2.write_at(1);
    max2.attach(storage);

    for (int i = 0; i < 50; ++i) {
        row[0] = i % 30;
        max2.reduce(row);
    }
    EXPECT_EQ(29, storage[1].asInt());

    max1.merge(max2);
    EXPECT_EQ(29, storage[0].asInt());
}

TEST(AggFieldTest, Min) {

    vector<uint32_t> offset({0, 1, 2});

    MemDataRow storage(offset);

    MemDataRow row(offset);

    IntMin min1(0);
    min1.write_at(0);
    min1.attach(storage);
    for (int i = 0; i < 50; ++i) {
        row[0] = (i % 20) - 40;
        min1.reduce(row);
    }
    EXPECT_EQ(-40, storage[0].asInt());

    IntMin min2(0);
    min2.write_at(1);
    min2.attach(storage);
    for (int i = 0; i < 50; ++i) {
        row[0] = i % 30 - 30;
        min2.reduce(row);
    }
    EXPECT_EQ(-30, storage[1].asInt());

    min1.merge(min2);
    EXPECT_EQ(-40, storage[0].asInt());
}

TEST(AggFieldTest, Avg) {

    vector<uint32_t> offset({0, 1, 2, 3, 4});

    MemDataRow storage(offset);

    MemDataRow row(offset);

    IntAvg avg1(0);
    avg1.write_at(0);
    avg1.attach(storage);
    for (int i = 0; i < 50; ++i) {
        row[0] = i;
        avg1.reduce(row);
    }
//    EXPECT_EQ(-40, storage[0].asInt());

    IntAvg avg2(0);
    avg2.write_at(2);
    avg2.attach(storage);
    for (int i = 0; i < 50; ++i) {
        row[0] = i + 50;
        avg2.reduce(row);
    }
//    EXPECT_EQ(-30, storage[1].asInt());

    avg1.merge(avg2);
    avg1.dump();
    EXPECT_EQ(49.5, storage[0].asDouble());
}

TEST(AggFieldTest, IntDistinct) {
    vector<uint32_t> offset({0, 1, 2, 3, 4});

    MemDataRow storage(offset);
    MemDataRow storage2(offset);

    MemDataRow row(offset);

    IntDistinctCount dcount(0);
    dcount.write_at(0);
    dcount.attach(storage);
    dcount.init();
    for (int i = 0; i < 50; ++i) {
        row[0] = i;
        dcount.reduce(row);
    }

    IntDistinctCount dcount2(0);
    dcount2.write_at(0);
    dcount2.attach(storage2);
    dcount2.init();
    for (int i = 0; i < 50; ++i) {
        row[0] = i + 150;
        dcount2.reduce(row);
    }
//    EXPECT_EQ(-30, storage[1].asInt());

    dcount.merge(dcount2);
    dcount.dump();
    EXPECT_EQ(100, storage[0].asInt());
}

TEST(AggReducerTest, Init) {
    auto header_copier = RowCopyFactory().from(RAW)->to(RAW)
            ->field(F_REGULAR, 0, 0)->field(F_REGULAR, 1, 1)
            ->layout_snapshot()->buildSnapshot();
    auto row_copier = RowCopyFactory().buildAssign(RAW, RAW, 4);
    AggReducer reducer(colOffset(4), header_copier.get(),
                       {new IntMax(2), new Count()}, vector<uint32_t>{2, 3});
    MemDataRow storage(colOffset(4));
    MemDataRow input(colOffset(4));

    input[0] = 5;
    input[1] = 100;
    input[2] = 33;
    reducer.attach(storage.raw());
    reducer.init(input);
    EXPECT_EQ(storage[0].asInt(), 5);
    EXPECT_EQ(storage[1].asInt(), 100);
    EXPECT_EQ(storage[2].asInt(), 33);
    EXPECT_EQ(storage[3].asInt(), 1);
}

TEST(AggReducerTest, Reduce) {
    auto header_copier = RowCopyFactory().from(RAW)->to(RAW)
            ->field(F_REGULAR, 0, 0)->field(F_REGULAR, 1, 1)
            ->layout_snapshot()->buildSnapshot();
    auto row_copier = RowCopyFactory().buildAssign(RAW, RAW, 4);
    AggReducer reducer(colOffset(4), header_copier.get(),
                       {new IntMax(2), new Count()}, vector<uint32_t>{2, 3});
    MemDataRow storage(colOffset(4));
    MemDataRow input(colOffset(4));

    input[0] = 5;
    input[1] = 100;
    input[2] = 33;
    reducer.attach(storage.raw());
    reducer.init(input);
    EXPECT_EQ(storage[0].asInt(), 5);
    EXPECT_EQ(storage[1].asInt(), 100);
    EXPECT_EQ(storage[2].asInt(), 33);
    EXPECT_EQ(storage[3].asInt(), 1);

    input[2] = 120;
    reducer.reduce(input);
    EXPECT_EQ(storage[0].asInt(), 5);
    EXPECT_EQ(storage[1].asInt(), 100);
    EXPECT_EQ(storage[2].asInt(), 120);
    EXPECT_EQ(storage[3].asInt(), 2);
}

TEST(AggReducerTest, Merge) {
    auto header_copier = RowCopyFactory().from(RAW)->to(RAW)
            ->field(F_REGULAR, 0, 0)->field(F_REGULAR, 1, 1)
            ->layout_snapshot()->buildSnapshot();
    auto row_copier = RowCopyFactory().buildAssign(RAW, RAW, 4);
    AggReducer reducer1(colOffset(4), header_copier.get(),
                        {new IntMax(2), new Count()}, vector<uint32_t>{2, 3});
    AggReducer reducer2(colOffset(4), header_copier.get(),
                        {new IntMax(2), new Count()}, vector<uint32_t>{2, 3});
    MemDataRow storage1(colOffset(4));
    MemDataRow storage2(colOffset(4));

    reducer1.attach(storage1.raw());
    reducer2.attach(storage2.raw());

    storage1[2] = 120;
    storage1[3] = 337;
    storage2[2] = 1341;
    storage2[3] = 2242;

    reducer1.merge(reducer2);

    EXPECT_EQ(1341, storage1[2].asInt());
    EXPECT_EQ(2579, storage1[3].asInt());
}

TEST(HashLargeCoreTest, Reduce) {
    auto header_copier = RowCopyFactory().from(RAW)->to(RAW)
            ->field(F_REGULAR, 0, 0)
            ->layout_snapshot()->buildSnapshot();
    auto row_copier = RowCopyFactory().buildAssign(RAW, RAW, 3);
    auto reducer = unique_ptr<AggReducer>(
            new AggReducer(colOffset(3), header_copier.get(),
                           {new IntMax(2), new Count()}, vector<uint32_t>{2, 3}));
    function<uint64_t(DataRow &)> hasher = [](DataRow &row) { return row[0].asInt(); };
    HashLargeCore core(colOffset(3), move(reducer), hasher, row_copier.get(), false);


    MemDataRow input(colOffset(3));
    input[0] = 5;
    input[1] = 4;
    input[2] = 100;
    core.reduce(input);

    input[0] = 5;
    input[1] = 4;
    input[2] = 120;
    core.reduce(input);

    input[0] = 7;
    input[1] = 4;
    input[2] = 100;
    core.reduce(input);

}

TEST(HashLargeCoreTest, Merge) {
    auto header_copier = RowCopyFactory().from(RAW)->to(RAW)
            ->field(F_REGULAR, 0, 0)->field(F_REGULAR, 1, 1)
            ->layout_snapshot()->buildSnapshot();
    auto row_copier = RowCopyFactory().buildAssign(RAW, RAW, 4);
    auto reducer1 = unique_ptr<AggReducer>(
            new AggReducer(colOffset(4), header_copier.get(),
                           {new IntMax(2), new Count()}, vector<uint32_t>{2, 3}));
    function<uint64_t(DataRow &)> hasher = [](DataRow &row) { return row[0].asInt(); };
    HashLargeCore core1(colOffset(4), move(reducer1), hasher, row_copier.get(), false);
    auto reducer2 = unique_ptr<AggReducer>(
            new AggReducer(colOffset(4), header_copier.get(),
                           {new IntMax(2), new Count()}, vector<uint32_t>{2, 3}));
    HashLargeCore core2(colOffset(4), move(reducer2), hasher, row_copier.get(), false);

    MemDataRow input(colOffset(3));
    input[0] = 5;
    input[1] = 4;
    input[2] = 100;
    core1.reduce(input);

    input[0] = 5;
    input[1] = 4;
    input[2] = 120;
    core1.reduce(input);

    input[0] = 7;
    input[1] = 4;
    input[2] = 100;
    core1.reduce(input);

    input[0] = 6;
    input[1] = 4;
    input[2] = 300;
    core2.reduce(input);

    input[0] = 5;
    input[1] = 4;
    input[2] = 500;
    core2.reduce(input);

    input[0] = 7;
    input[1] = 4;
    input[2] = 900;
    core2.reduce(input);

    core1.merge(core2);
}

TEST(HashLargeCoreTest, Dump) {
    auto header_copier = RowCopyFactory().from(RAW)->to(RAW)
            ->field(F_REGULAR, 0, 0)->layout_snapshot()->buildSnapshot();
    auto row_copier = RowCopyFactory().buildAssign(RAW, RAW, 3);
    auto reducer = unique_ptr<AggReducer>(
            new AggReducer(colOffset(3), header_copier.get(),
                           {new IntMax(2), new Count()}, vector<uint32_t>{1, 2}));
    function<uint64_t(DataRow &)> hasher = [](DataRow &row) { return row[0].asInt(); };
    HashLargeCore core(colOffset(3), move(reducer), hasher, row_copier.get(), false);

    MemDataRow input(colOffset(3));
    input[0] = 5;
    input[1] = 4;
    input[2] = 100;
    core.reduce(input);

    input[0] = 6;
    input[1] = 4;
    input[2] = 300;
    core.reduce(input);

    input[0] = 7;
    input[1] = 4;
    input[2] = 100;
    core.reduce(input);

    input[0] = 5;
    input[1] = 4;
    input[2] = 90;
    core.reduce(input);

    input[0] = 6;
    input[1] = 4;
    input[2] = 310;
    core.reduce(input);

    input[0] = 6;
    input[1] = 4;
    input[2] = 30;
    core.reduce(input);

    auto table = MemTable::Make(3);

    core.dump(*table, nullptr);
    auto block = (*table->blocks()->collect())[0];

    EXPECT_EQ(3, block->size());
    auto rows = block->rows();
    DataRow &next = rows->next();
    EXPECT_EQ(5, next[0].asInt());
    EXPECT_EQ(100, next[1].asInt());
    EXPECT_EQ(2, next[2].asInt());

    next = rows->next();
    EXPECT_EQ(6, next[0].asInt());
    EXPECT_EQ(310, next[1].asInt());
    EXPECT_EQ(3, next[2].asInt());

    next = rows->next();
    EXPECT_EQ(7, next[0].asInt());
    EXPECT_EQ(100, next[1].asInt());
    EXPECT_EQ(1, next[2].asInt());
}

TEST(RecordingMinTest, Reduce) {
    RecordingIntMin rim(0, 1);
    rim.write_at(0);
    MemDataRow storage(colOffset(3));
    MemDataRow input(colOffset(3));
    rim.attach(storage);
    rim.init();

    input[0] = 100;
    input[1] = 33;
    rim.reduce(input);

    input[0] = 120;
    input[1] = 37;
    rim.reduce(input);

    input[0] = 21;
    input[1] = 99;
    rim.reduce(input);

    input[0] = 21;
    input[1] = 122;
    rim.reduce(input);

    input[0] = 121;
    input[1] = 337;
    rim.reduce(input);

    auto keys = rim.keys();
    EXPECT_EQ(storage[0].asInt(), 21);
    EXPECT_EQ(keys->size(), 2);
    auto ite = keys->begin();
    EXPECT_EQ(*(ite++), 122);
    EXPECT_EQ(*(ite), 99);

    delete keys;
}

TEST(RecordingMinTest, Merge) {
    RecordingIntMin rim1(0, 1);
    rim1.write_at(0);
    MemDataRow storage1(colOffset(3));

    RecordingIntMin rim2(0, 1);
    rim2.write_at(0);
    MemDataRow storage2(colOffset(3));
    MemDataRow input(colOffset(3));

    rim1.attach(storage1);
    rim1.init();
    storage1[0] = 120;
    auto keys1 = rim1.keys();
    keys1->insert(221);
    keys1->insert(222);

    rim2.attach(storage2);
    rim2.init();
    storage2[0] = 221;
    auto keys2 = rim2.keys();
    keys2->insert(32);
    keys2->insert(33);

    rim1.merge(rim2);
    EXPECT_EQ(storage1[0].asInt(), 120);
    EXPECT_EQ(keys1->size(), 2);
    auto ite = keys1->begin();
    EXPECT_EQ(*(ite++), 222);
    EXPECT_EQ(*ite, 221);

    rim2.init();
    storage2[0] = 11;
    keys2 = rim2.keys();
    keys2->insert(32);
    keys2->insert(33);
    rim1.merge(rim2);
    EXPECT_EQ(storage1[0].asInt(), 11);
    EXPECT_EQ(keys1->size(), 2);
    ite = keys1->begin();
    EXPECT_EQ(*(ite++), 32);
    EXPECT_EQ(*ite, 33);

    rim2.init();
    storage2[0] = 11;
    keys2 = rim2.keys();
    keys2->clear();
    keys2->insert(101);
    keys2->insert(102);
    rim1.merge(rim2);
    EXPECT_EQ(storage1[0].asInt(), 11);
    EXPECT_EQ(keys1->size(), 4);
    ite = keys1->begin();
    EXPECT_EQ(*(ite++), 101);
    EXPECT_EQ(*(ite++), 33);
    EXPECT_EQ(*(ite++), 102);
    EXPECT_EQ(*(ite++), 32);

    delete rim1.keys();
}

TEST(RecordingHashCoreTest, Dump) {
    auto header_copier = RowCopyFactory().from(RAW)->to(RAW)
            ->field(F_REGULAR, 0, 0)
            ->layout_snapshot()->buildSnapshot();
    auto row_copier = RowCopyFactory().buildAssign(RAW, RAW, 3);
    function<uint64_t(DataRow &)> hasher = [](DataRow &row) { return row[0].asInt(); };
    function<unique_ptr<AggReducer>()> rcgen = [&header_copier]() {
        return unique_ptr<AggReducer>(new AggReducer(colOffset(3), header_copier.get(),
                                                     new RecordingIntMin(1, 2), 1));
    };
    RecordingHashCore core(colOffset(3), rcgen, hasher, row_copier.get());


    MemDataRow input(colOffset(3));
    input[0] = 5;
    input[1] = 33;
    input[2] = 127;
    core.reduce(input);
    input[0] = 2;
    input[1] = 34;
    input[2] = 227;
    core.reduce(input);
    input[0] = 5;
    input[1] = 20;
    input[2] = 120;
    core.reduce(input);
    input[0] = 5;
    input[1] = 20;
    input[2] = 122;
    core.reduce(input);
    input[0] = 9;
    input[1] = 20;
    input[2] = 221;
    core.reduce(input);

    auto mtable = MemTable::Make(3);
    core.dump(*mtable, nullptr);

    auto block = (*mtable->blocks()->collect())[0];
    EXPECT_EQ(block->size(), 4);
    auto rows = block->rows();
    DataRow &row = rows->next();
    EXPECT_EQ(5, row[0].asInt());
    EXPECT_EQ(20, row[1].asInt());
    EXPECT_EQ(122, row[2].asInt());
    row = rows->next();
    EXPECT_EQ(5, row[0].asInt());
    EXPECT_EQ(20, row[1].asInt());
    EXPECT_EQ(120, row[2].asInt());
    row = rows->next();
    EXPECT_EQ(9, row[0].asInt());
    EXPECT_EQ(20, row[1].asInt());
    EXPECT_EQ(221, row[2].asInt());
    row = rows->next();
    EXPECT_EQ(2, row[0].asInt());
    EXPECT_EQ(34, row[1].asInt());
    EXPECT_EQ(227, row[2].asInt());
}

TEST(RecordingHashCoreTest, DumpVertical) {
    auto header_copier = RowCopyFactory().from(RAW)->to(RAW)
            ->field(F_REGULAR, 0, 0)
            ->layout_snapshot()->buildSnapshot();
    auto row_copier = RowCopyFactory().buildAssign(RAW, OTHER, 3);
    function<uint64_t(DataRow &)> hasher = [](DataRow &row) { return row[0].asInt(); };
    function<unique_ptr<AggReducer>()> rcgen = [&header_copier]() {
        return unique_ptr<AggReducer>(new AggReducer(colOffset(3), header_copier.get(),
                                                     new RecordingIntMin(1, 2), 1));
    };
    RecordingHashCore core(colOffset(3), rcgen, hasher, row_copier.get());


    MemDataRow input(colOffset(3));
    input[0] = 5;
    input[1] = 33;
    input[2] = 127;
    core.reduce(input);
    input[0] = 2;
    input[1] = 34;
    input[2] = 227;
    core.reduce(input);
    input[0] = 5;
    input[1] = 20;
    input[2] = 120;
    core.reduce(input);
    input[0] = 5;
    input[1] = 20;
    input[2] = 122;
    core.reduce(input);
    input[0] = 9;
    input[1] = 20;
    input[2] = 221;
    core.reduce(input);

    auto mtable = MemTable::Make(3, true);
    core.dump(*mtable, nullptr);

    auto block = (*mtable->blocks()->collect())[0];
    auto vblock = dynamic_pointer_cast<MemvBlock>(block);
    EXPECT_TRUE(vblock != nullptr);

    EXPECT_EQ(block->size(), 4);
    auto rows = block->rows();
    DataRow &row = rows->next();
    EXPECT_EQ(5, row[0].asInt());
    EXPECT_EQ(20, row[1].asInt());
    EXPECT_EQ(122, row[2].asInt());
    row = rows->next();
    EXPECT_EQ(5, row[0].asInt());
    EXPECT_EQ(20, row[1].asInt());
    EXPECT_EQ(120, row[2].asInt());
    row = rows->next();
    EXPECT_EQ(9, row[0].asInt());
    EXPECT_EQ(20, row[1].asInt());
    EXPECT_EQ(221, row[2].asInt());
    row = rows->next();
    EXPECT_EQ(2, row[0].asInt());
    EXPECT_EQ(34, row[1].asInt());
    EXPECT_EQ(227, row[2].asInt());
}

TEST(HashLargeAggTest, Agg) {
    function<uint64_t(DataRow &)> hasher =
            [](DataRow &row) {
                return row[0].asInt();
            };

    function<vector<AggField *>()> aggFields = []() {
        return vector<AggField *>{new DoubleSum(1), new Count()};
    };

    HashLargeAgg agg(hasher, RowCopyFactory().field(F_REGULAR, 0, 0)->buildSnapshot(),
                     aggFields);

    auto memTable = MemTable::Make(3);

    auto block1 = memTable->allocate(100);
    auto block2 = memTable->allocate(100);

    auto row1 = block1->rows();
    auto row2 = block2->rows();

    srand(time(NULL));

    vector<int> count(20, 0);
    vector<double> sum(20, 0);

    for (int i = 0; i < 100; i++) {
        int r1idx = i % 10;
        int r2idx = i % 20;
        double v1 = (double) rand() / RAND_MAX;
        double v2 = (double) rand() / RAND_MAX;
        (*row1)[i][0] = r1idx;
        (*row2)[i][0] = r2idx;
        (*row1)[i][1] = v1;
        (*row2)[i][1] = v2;
        sum[r1idx] += v1;
        sum[r2idx] += v2;
        count[r1idx] += 1;
        count[r2idx] += 1;
    }

    auto aggtable = agg.agg(*memTable);
    auto agged = aggtable->blocks()->collect();

    EXPECT_EQ(1, agged->size());
    auto aggblock = (*agged)[0];
    EXPECT_EQ(20, aggblock->size());
    auto rows = aggblock->rows();
    for (int i = 0; i < 20; ++i) {
        auto i_idx = (*rows)[i][0].asInt();
        auto i_sum = (*rows)[i][1].asDouble();
        auto i_count = (*rows)[i][2].asInt();
        EXPECT_EQ(i_count, count[i_idx]);
        EXPECT_DOUBLE_EQ(i_sum, sum[i_idx]);
    }
    rows = aggblock->rows();
    for (int i = 0; i < 20; ++i) {
        DataRow &row = rows->next();
        auto i_idx = row[0].asInt();
        auto i_sum = row[1].asDouble();
        auto i_count = row[2].asInt();
        EXPECT_EQ(i_count, count[i_idx]);
        EXPECT_DOUBLE_EQ(i_sum, sum[i_idx]);
    }
}

TEST(HashAggTest, Agg) {
    function<uint64_t(DataRow &)> hasher =
            [](DataRow &row) {
                return row[0].asInt();
            };

    function<vector<AggField *>()> aggFields = []() {
        return vector<AggField *>{new DoubleSum(1), new Count()};
    };

    HashAgg agg(hasher, RowCopyFactory().field(F_REGULAR, 0, 0)->buildSnapshot(),
                aggFields);

    auto memTable = MemTable::Make(3);

    auto block1 = memTable->allocate(100);
    auto block2 = memTable->allocate(100);

    auto row1 = block1->rows();
    auto row2 = block2->rows();

    srand(time(NULL));

    vector<int> count(20, 0);
    vector<double> sum(20, 0);

    for (int i = 0; i < 100; i++) {
        int r1idx = i % 10;
        int r2idx = i % 20;
        double v1 = (double) rand() / RAND_MAX;
        double v2 = (double) rand() / RAND_MAX;
        (*row1)[i][0] = r1idx;
        (*row2)[i][0] = r2idx;
        (*row1)[i][1] = v1;
        (*row2)[i][1] = v2;
        sum[r1idx] += v1;
        sum[r2idx] += v2;
        count[r1idx] += 1;
        count[r2idx] += 1;
    }

    auto aggtable = agg.agg(*memTable);
    auto agged = aggtable->blocks()->collect();

    EXPECT_EQ(1, agged->size());
    auto aggblock = (*agged)[0];
    EXPECT_EQ(20, aggblock->size());
    auto rows = aggblock->rows();
    for (int i = 0; i < 20; ++i) {
        auto i_idx = (*rows)[i][0].asInt();
        auto i_sum = (*rows)[i][1].asDouble();
        auto i_count = (*rows)[i][2].asInt();
        EXPECT_EQ(i_count, count[i_idx]);
        EXPECT_DOUBLE_EQ(i_sum, sum[i_idx]);
    }
    rows = aggblock->rows();
    set<int32_t> key_count;
    for (int i = 0; i < 20; ++i) {
        DataRow &row = rows->next();
        auto i_idx = row[0].asInt();
        key_count.insert(i_idx);
        auto i_sum = row[1].asDouble();
        auto i_count = row[2].asInt();
        EXPECT_EQ(i_count, count[i_idx]);
        EXPECT_DOUBLE_EQ(i_sum, sum[i_idx]);
    }
    EXPECT_EQ(20, key_count.size());
}

TEST(TableAggTest, Agg) {
    function<uint32_t(DataRow &)> hasher =
            [](DataRow &row) {
                return row[0].asInt();
            };

    function<vector<AggField *>()> aggFields = []() {
        return vector<AggField *>{new DoubleSum(1), new Count()};
    };

    TableAgg agg(10, hasher, RowCopyFactory().field(F_REGULAR, 0, 0)->buildSnapshot(),
                 aggFields);

    auto memTable = MemTable::Make(3);

    auto block1 = memTable->allocate(100);
    auto block2 = memTable->allocate(100);

    auto row1 = block1->rows();
    auto row2 = block2->rows();

    srand(time(NULL));

    vector<int> count(10, 0);
    vector<double> sum(10, 0);

    for (int i = 0; i < 100; i++) {
        int r1idx = i % 5;
        int r2idx = i % 10;
        double v1 = (double) rand() / RAND_MAX;
        double v2 = (double) rand() / RAND_MAX;
        (*row1)[i][0] = r1idx;
        (*row2)[i][0] = r2idx;
        (*row1)[i][1] = v1;
        (*row2)[i][1] = v2;
        sum[r1idx] += v1;
        sum[r2idx] += v2;
        count[r1idx] += 1;
        count[r2idx] += 1;
    }

    auto aggtable = agg.agg(*memTable);
    auto agged = aggtable->blocks()->collect();

    EXPECT_EQ(1, agged->size());
    auto aggblock = (*agged)[0];
    EXPECT_EQ(10, aggblock->size());
    auto rows = aggblock->rows();
    for (int i = 0; i < 10; ++i) {
        auto i_idx = (*rows)[i][0].asInt();
        auto i_sum = (*rows)[i][1].asDouble();
        auto i_count = (*rows)[i][2].asInt();
        EXPECT_EQ(i_count, count[i_idx]);
        EXPECT_DOUBLE_EQ(i_sum, sum[i_idx]);
    }
    rows = aggblock->rows();
    set<int32_t> key_count;
    for (int i = 0; i < 10; ++i) {
        DataRow &row = rows->next();
        auto i_idx = row[0].asInt();
        key_count.insert(i_idx);
        auto i_sum = row[1].asDouble();
        auto i_count = row[2].asInt();
        EXPECT_EQ(i_count, count[i_idx]);
        EXPECT_DOUBLE_EQ(i_sum, sum[i_idx]);
    }
    EXPECT_EQ(key_count.size(), 10);
}


using namespace lqf::agg::recording;

TEST(RecordingHashAggTest, AggRecording) {
    function<uint64_t(DataRow &)> hasher =
            [](DataRow &row) {
                return row[0].asInt();
            };

    RecordingHashAgg agg(hasher, RowCopyFactory().field(F_REGULAR, 0, 0)->buildSnapshot(), []() {
        return new RecordingIntMin(1, 2);
    });

    auto memTable = MemTable::Make(3);

    auto block1 = memTable->allocate(100);
    auto block2 = memTable->allocate(100);

    auto row1 = block1->rows();
    auto row2 = block2->rows();

    for (int i = 0; i < 100; i++) {
        (*row1)[i][0] = 0;
        (*row1)[i][2] = i;
        (*row1)[i][1] = i % 30;
        (*row2)[i][0] = 1;
        (*row2)[i][2] = i;
        (*row2)[i][1] = (i % 40) + 5;
    }

    auto aggtable = agg.agg(*memTable);
    auto agged = aggtable->blocks()->collect();

    EXPECT_EQ(1, agged->size());
    auto aggblock = (*agged)[0];
    EXPECT_EQ(7, aggblock->size());
    auto rows = aggblock->rows();

    DataRow &row = rows->next();
    EXPECT_EQ(row[0].asInt(), 1);
    EXPECT_EQ(row[1].asInt(), 5);
    EXPECT_EQ(row[2].asInt(), 80);
    row = rows->next();
    EXPECT_EQ(row[0].asInt(), 1);
    EXPECT_EQ(row[1].asInt(), 5);
    EXPECT_EQ(row[2].asInt(), 0);
    row = rows->next();
    EXPECT_EQ(row[0].asInt(), 1);
    EXPECT_EQ(row[1].asInt(), 5);
    EXPECT_EQ(row[2].asInt(), 40);
    row = rows->next();
    EXPECT_EQ(row[0].asInt(), 0);
    EXPECT_EQ(row[1].asInt(), 0);
    EXPECT_EQ(row[2].asInt(), 90);
    row = rows->next();
    EXPECT_EQ(row[0].asInt(), 0);
    EXPECT_EQ(row[1].asInt(), 0);
    EXPECT_EQ(row[2].asInt(), 60);
    row = rows->next();
    EXPECT_EQ(row[0].asInt(), 0);
    EXPECT_EQ(row[1].asInt(), 0);
    EXPECT_EQ(row[2].asInt(), 0);
    row = rows->next();
    EXPECT_EQ(row[0].asInt(), 0);
    EXPECT_EQ(row[1].asInt(), 0);
    EXPECT_EQ(row[2].asInt(), 30);
}

TEST(SimpleAggTest, Agg) {
    SimpleAgg agg([]() { return vector<AggField *>{new DoubleSum(1), new Count(), new IntMax(0)}; });

    auto memTable = MemTable::Make(3);

    auto block1 = memTable->allocate(100);
    auto block2 = memTable->allocate(100);

    auto row1 = block1->rows();
    auto row2 = block2->rows();

    double sum = 0;
    int count = 0;
    int max = INT32_MIN;

    srand(time(NULL));

    for (int i = 0; i < 100; i++) {
        int v00 = rand();
        double v01 = static_cast<double>(rand()) / RAND_MAX;
        (*row1)[i][0] = v00;
        (*row1)[i][1] = v01;

        int v10 = rand();
        double v11 = static_cast<double>(rand()) / RAND_MAX;
        (*row2)[i][0] = v10;
        (*row2)[i][1] = v11;

        sum += v01 + v11;
        count += 2;
        max = std::max(std::max(max, v00), v10);
    }

    auto aggtable = agg.agg(*memTable);
    auto agged = aggtable->blocks()->collect();

    EXPECT_EQ(1, agged->size());
    auto aggblock = (*agged)[0];
    EXPECT_EQ(1, aggblock->size());
    auto rows = aggblock->rows();
    EXPECT_NEAR(sum, (*rows)[0][0].asDouble(), 0.0000001);
    EXPECT_EQ(count, (*rows)[0][1].asInt());
    EXPECT_EQ(max, (*rows)[0][2].asInt());
}

TEST(RecordingSimpleAggTest, AggRecording) {

    RecordingSimpleAgg agg([]() { return new RecordingIntMin(1, 0); });

    auto memTable = MemTable::Make(2);

    auto block1 = memTable->allocate(100);
    auto block2 = memTable->allocate(100);

    auto row1 = block1->rows();
    auto row2 = block2->rows();

    for (int i = 0; i < 100; i++) {
        (*row1)[i][0] = i;
        (*row1)[i][1] = i % 30;
        (*row2)[i][0] = i;
        (*row2)[i][1] = i % 40;
    }

    auto aggtable = agg.agg(*memTable);
    auto agged = aggtable->blocks()->collect();

    EXPECT_EQ(1, agged->size());
    auto aggblock = (*agged)[0];
    EXPECT_EQ(6, aggblock->size());
    auto rows = aggblock->rows();

    DataRow &row = rows->next();
    EXPECT_EQ(row[0].asInt(), 0);
    EXPECT_EQ(row[1].asInt(), 40);
    row = rows->next();
    EXPECT_EQ(row[0].asInt(), 0);
    EXPECT_EQ(row[1].asInt(), 80);
    row = rows->next();
    EXPECT_EQ(row[0].asInt(), 0);
    EXPECT_EQ(row[1].asInt(), 30);
    row = rows->next();
    EXPECT_EQ(row[0].asInt(), 0);
    EXPECT_EQ(row[1].asInt(), 0);
    row = rows->next();
    EXPECT_EQ(row[0].asInt(), 0);
    EXPECT_EQ(row[1].asInt(), 60);
    row = rows->next();
    EXPECT_EQ(row[0].asInt(), 0);
    EXPECT_EQ(row[1].asInt(), 90);
}

TEST(StripeHashAggTest, agg) {
    function<uint64_t(DataRow &)> hasher = [](DataRow &row) {
        return row[2].asInt();
    };
    function<uint64_t(DataRow &)> stripe_hasher = [](DataRow &row) {
        return row[0].asInt();
    };
    // 0 1 2 3 4 -> 2 1 0
    StripeHashAgg agg(32, hasher, stripe_hasher,
                      RowCopyFactory().field(F_REGULAR, 2, 0)
                              ->field(F_REGULAR, 1, 1)->field(F_REGULAR, 0, 2)->buildSnapshot(),
                      RowCopyFactory().field(F_REGULAR, 0, 0)->buildSnapshot(),
                      []() { return vector<AggField *>{new Count(), new IntSum(1), new DoubleMax(2)}; });

    vector<int> count(50, 0);
    vector<int> sum(50, 0);
    vector<double> max(50, 0);

    srand(time(NULL));

    auto input = MemTable::Make(5);
    for (auto i = 0; i < 5; ++i) {
        auto block = input->allocate(150);
        auto rows = block->rows();
        for (auto j = 0; j < 150; ++j) {
            int head = abs(rand()) % 50;
            int intval = rand() % 1000;
            double doubleval = static_cast<double>(rand()) / RAND_MAX;
            count[head] += 1;
            sum[head] += intval;
            max[head] = std::max(max[head], doubleval);
            DataRow &next = rows->next();
            next[2] = head;
            next[1] = intval;
            next[0] = doubleval;
        }
    }

    auto result = agg.agg(*input);
    auto blocks = result->blocks()->collect();
    EXPECT_EQ(blocks->size(), 32);

    for (auto &block:*blocks) {
        auto rows = block->rows();
        auto block_size = block->size();
        for (auto i = 0u; i < block_size; ++i) {
            DataRow &row = rows->next();
            auto key = row[0].asInt();
            EXPECT_EQ(row[1].asInt(), count[key]) << key;
            EXPECT_EQ(row[2].asInt(), sum[key]) << key;
            EXPECT_EQ(row[3].asDouble(), max[key]) << key;
        }
    }
}