//
// Created by harper on 2/24/20.
//

#include <gtest/gtest.h>
#include "filter.h"
#include <iostream>
#include "data_model.h"
#include "print.h"

using namespace std;
using namespace lqf;

class ColFilterTest : public ::testing::Test {
protected:
    shared_ptr<ParquetFileReader> fileReader_;
    shared_ptr<RowGroupReader> rowGroup_;
public:
    virtual void SetUp() override {
        fileReader_ = ParquetFileReader::OpenFile("testres/lineitem");
        rowGroup_ = fileReader_->RowGroup(0);
        return;
    }
};

TEST(RowFilterTest, Filter) {
    auto memTable = MemTable::Make(5);
    auto block1 = memTable->allocate(100);
    auto block2 = memTable->allocate(100);

    auto row1 = block1->rows();
    auto row2 = block2->rows();
    for (int i = 0; i < 100; ++i) {
        (*row1)[i][0] = i;
        (*row1)[i][1] = static_cast<double>((i - 50) * (i - 50) * 0.05);
        (*row2)[i][0] = (i + 3) * i + 1;
    }

    function<bool(DataRow &)> pred = [](DataRow &row) {
        return row[0].asInt() % 5 == 0;
    };

    RowFilter rowFilter(pred);
    auto filteredTable = rowFilter.filter(*memTable);

    auto filteredBlocks = filteredTable->blocks()->collect();

    auto fblock1 = (*filteredBlocks)[0];
    auto fblock2 = (*filteredBlocks)[1];

    EXPECT_EQ(20, fblock1->size());
    EXPECT_EQ(20, fblock2->size());
}

using namespace lqf;

TEST_F(ColFilterTest, FilterOnSimpleCol) {
    auto ptable = ParquetTable::Open("testres/lineitem", 7);
    initializer_list<ColPredicate *> list = {
            new SimplePredicate(0, [](const DataField &field) { return field.asInt() % 10 == 0; })};
    auto filter = make_shared<ColFilter>(list);

    auto filtered = filter->filter(*ptable);
    auto filteredblocks = filtered->blocks()->collect();

    auto table2 = ParquetTable::Open("testres/lineitem", 7);
    auto rawblocks = table2->blocks()->collect();

    EXPECT_EQ(filteredblocks->size(), rawblocks->size());

    for (uint32_t i = 0; i < rawblocks->size(); ++i) {
        auto fb = (*filteredblocks)[i];
        auto rb = (*rawblocks)[i];

        auto bitmap = make_shared<SimpleBitmap>(rb->size());
        auto rbrows = rb->rows();
        for (uint32_t j = 0; j < rb->size(); ++j) {
            if ((*rbrows)[j][0].asInt() % 10 == 0) {
                bitmap->put(j);
            }
        }
        EXPECT_EQ(fb->size(), bitmap->cardinality());

        auto fbrows = fb->rows();
        auto bite = bitmap->iterator();

        for (uint32_t bidx = 0; bidx < bitmap->cardinality(); ++bidx) {
            fbrows->next();
            EXPECT_EQ(fbrows->pos(), bite->next());
        }
    }
}

using namespace lqf::sboost;

TEST_F(ColFilterTest, FilterSboost) {
    auto ptable = ParquetTable::Open("testres/lineitem", (1 << 14) - 1);

    function<bool(const ByteArray &)> pred = [](const ByteArray &input) {
        return !strncmp(reinterpret_cast<const char *>(input.ptr + input.len - 3), "AIL", 3);
    };
    function<bool(const DataField &)> pred2 = [](const DataField &field) {
        ByteArray &input = field.asByteArray();
        return !strncmp(reinterpret_cast<const char *>(input.ptr + input.len - 3), "AIL", 3);
    };
    ColFilter filter({new SboostPredicate<ByteArrayType>(14, bind(&ByteArrayDictMultiEq::build, pred))});

    auto filtered = filter.filter(*ptable)->blocks()->collect();
    auto result = (*filtered)[0];

    ColFilter regFilter({new SimplePredicate(14, pred2)});
    auto filtered2 = regFilter.filter(*ptable)->blocks()->collect();
    auto result2 = (*filtered2)[0];

    auto raw1 = dynamic_pointer_cast<SimpleBitmap>(dynamic_pointer_cast<MaskedBlock>(result)->mask())->raw();
    auto raw2 = dynamic_pointer_cast<SimpleBitmap>(dynamic_pointer_cast<MaskedBlock>(result2)->mask())->raw();

    EXPECT_EQ(result2->size(), result->size());
    for (uint32_t i = 0; i < 94; i++) {
        EXPECT_EQ(raw1[i], raw2[i]) << i;
    }

}

TEST_F(ColFilterTest, FilterMultiSboost) {
    auto ptable = ParquetTable::Open("testres/lineitem", (1 << 14) - 1);

    function<bool(const ByteArray &)> sbstPred = [](const ByteArray &input) {
        return !strncmp(reinterpret_cast<const char *>(input.ptr + input.len - 3), "AIL", 3);
    };

    ColFilter filter({new SboostPredicate<ByteArrayType>(14, bind(&ByteArrayDictMultiEq::build, sbstPred))});

    ByteArray filter2Pred("AIR");
    ColFilter filter2({new SboostPredicate<ByteArrayType>(14, bind(&ByteArrayDictEq::build, filter2Pred))});

    auto sbFiltered = filter.filter(*ptable);
    auto sbFiltered2 = filter2.filter(*ptable);

    auto sbResult = (*sbFiltered->blocks()->collect())[0];
    auto sbResult2 = (*sbFiltered2->blocks()->collect())[0];


    function<bool(const DataField &)> simplePred = [](const DataField &field) {
        ByteArray &input = field.asByteArray();
        return !strncmp(reinterpret_cast<const char *>(input.ptr + input.len - 3), "AIL", 3);
    };
    function<bool(const DataField &)> simplePred2 = [](const DataField &field) {
        ByteArray &input = field.asByteArray();
        return input.len == 3 && !strncmp(reinterpret_cast<const char *>(input.ptr), "AIR", 3);
    };
    ColFilter regFilter({new SimplePredicate(14, simplePred)});
    auto simpleFiltered = regFilter.filter(*ptable)->blocks()->collect();
    auto simpleResult = (*simpleFiltered)[0];

    ColFilter regFilter2({new SimplePredicate(14, simplePred2)});
    auto simpleFiltered2 = regFilter2.filter(*ptable)->blocks()->collect();
    auto simpleResult2 = (*simpleFiltered2)[0];

    auto sbraw1 = dynamic_pointer_cast<SimpleBitmap>(dynamic_pointer_cast<MaskedBlock>(sbResult)->mask())->raw();
    auto sbraw2 = dynamic_pointer_cast<SimpleBitmap>(dynamic_pointer_cast<MaskedBlock>(sbResult2)->mask())->raw();
    auto simraw1 = dynamic_pointer_cast<SimpleBitmap>(dynamic_pointer_cast<MaskedBlock>(simpleResult)->mask())->raw();
    auto simraw2 = dynamic_pointer_cast<SimpleBitmap>(dynamic_pointer_cast<MaskedBlock>(simpleResult2)->mask())->raw();

    EXPECT_EQ(sbResult->size(), simpleResult->size());
    for (uint32_t i = 0; i < 94; i++) {
        EXPECT_EQ(sbraw1[i], simraw1[i]) << i;
    }
    EXPECT_EQ(sbResult2->size(), simpleResult2->size());
    for (uint32_t i = 0; i < 94; i++) {
        EXPECT_EQ(sbraw2[i], simraw2[i]) << i;
    }
}

TEST(SboostRowFilterTest, Filter) {
    auto ptable = ParquetTable::Open("testres/lineitem2", (1 << 14) - 1);

    SboostRowFilter filter(10, 11);
    auto result = filter.filter(*ptable);

    RowFilter compareFilter([](DataRow &drow) {
        return drow[10].asByteArray() < drow[11].asByteArray();
    });
    auto result2 = compareFilter.filter(*ptable);

    auto b1 = (*(result->blocks()->collect()))[0];
    auto b2 = (*(result2->blocks()->collect()))[0];

    EXPECT_EQ(b1->size(), b2->size());
}

TEST(SboostRow2FilterTest, Filter) {
    auto ptable = ParquetTable::Open("testres/lineitem2", (1 << 14) - 1);

    SboostRow2Filter filter(10, 11, 12);
    auto result = filter.filter(*ptable);

    RowFilter compareFilter([](DataRow &drow) {
        return (drow[10].asByteArray() < drow[11].asByteArray()) && (drow[11].asByteArray() < drow[12].asByteArray());
    });
    auto result2 = compareFilter.filter(*ptable);

    auto b1 = (*(result->blocks()->collect()))[0];
    auto b2 = (*(result2->blocks()->collect()))[0];

    auto mb1 = dynamic_pointer_cast<MaskedBlock>(b1);
    auto mb2 = dynamic_pointer_cast<MaskedBlock>(b2);

    auto mask1 = dynamic_pointer_cast<SimpleBitmap>(mb1->mask());
    auto mask2 = dynamic_pointer_cast<SimpleBitmap>(mb2->mask());

    for (uint32_t i = 0; i < 157; ++i) {
        EXPECT_EQ(mask1->raw()[i], mask2->raw()[i]) << i;
    }

//    EXPECT_EQ(b1->size(), b2->size());
}