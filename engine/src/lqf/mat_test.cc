//
// Created by harper on 3/17/20.
//

#include <gtest/gtest.h>
#include "mat.h"
#include "filter.h"
#include "rowcopy.h"

using namespace lqf;

//TEST(MemMatTest, Mat) {
//
//    MemMat mat(3, MLB MI(0, 0)
//        MI(1, 1)
//        MD(5, 2) MLE);
//
//    auto parquetTable = ParquetTable::Open("testres/lineitem", 0x27);
//    auto matted = mat.mat(*parquetTable);
//    auto content = matted->blocks()->collect();
//
//    EXPECT_EQ(1, content->size());
//
//    EXPECT_EQ(6005, (*content)[0]->size());
//    auto rows = (*content)[0]->rows();
//
//    EXPECT_EQ(1, (*rows)[0][0].asInt());
//    EXPECT_EQ(156, (*rows)[0][1].asInt());
//    EXPECT_NEAR(17954.5499, (*rows)[0][2].asDouble(), 0.001);
//    EXPECT_EQ(1, (*rows)[2][0].asInt());
//    EXPECT_EQ(64, (*rows)[2][1].asInt());
//    EXPECT_NEAR(7712.4799, (*rows)[2][2].asDouble(), 0.001);
//    EXPECT_EQ(3, (*rows)[10][0].asInt());
//    EXPECT_EQ(30, (*rows)[10][1].asInt());
//    EXPECT_NEAR(1860.0599, (*rows)[10][2].asDouble(), 0.001);
//}

TEST(FilterMatTest, Mat) {

    auto ptable = ParquetTable::Open("testres/lineitem", 0x7);

    function<bool(const DataField &)> pred = [](const DataField &field) {
        return field.asInt() % 10 == 0;
    };
    ColFilter filter({new SimplePredicate(0, pred)});

    auto filtered = filter.filter(*ptable);

    FilterMat fmat;
    auto ftable = fmat.mat(*filtered);

    auto f1 = ftable->blocks()->collect();
    auto f2 = ftable->blocks()->collect();
    auto f3 = ftable->blocks()->collect();

    EXPECT_EQ(585, (*f1)[0]->size());
    EXPECT_EQ(585, (*f2)[0]->size());
    EXPECT_EQ(585, (*f3)[0]->size());
}

using namespace lqf::rowcopy;

TEST(HashMatTest, Mat) {
    auto ptable = ParquetTable::Open("testres/nation", 7);

    HashMat mat(0, RowCopyFactory().from(EXTERNAL)->to(RAW)->field(F_STRING, 1, 0)->buildSnapshot());

    auto result = mat.mat(*ptable);
    EXPECT_EQ(result->colSize(), vector<uint32_t>({2}));
    auto blocks = result->blocks()->collect();
    EXPECT_EQ(1, blocks->size());
    auto block = (*blocks)[0];
//    return;
    auto mblock = dynamic_pointer_cast<HashMemBlock<Hash32Container>>(block);
    auto container = mblock->content();
    EXPECT_EQ(container->size(), 25);
    auto ite = container->iterator();
    ite->hasNext();
    EXPECT_EQ((ite->next().second)[0].asByteArray(), ByteArray("ALGERIA"));
    ite->hasNext();
    EXPECT_EQ((ite->next().second)[0].asByteArray(), ByteArray("MOROCCO"));
    ite->hasNext();
    EXPECT_EQ((ite->next().second)[0].asByteArray(), ByteArray("IRAQ"));
    ite->hasNext();
    EXPECT_EQ((ite->next().second)[0].asByteArray(), ByteArray("GERMANY"));
    ite->hasNext();
    EXPECT_EQ((ite->next().second)[0].asByteArray(), ByteArray("RUSSIA"));
    ite->hasNext();
    EXPECT_EQ((ite->next().second)[0].asByteArray(), ByteArray("CANADA"));
    ite->hasNext();
    EXPECT_EQ((ite->next().second)[0].asByteArray(), ByteArray("CHINA"));
    ite->hasNext();
    EXPECT_EQ((ite->next().second)[0].asByteArray(), ByteArray("KENYA"));
    ite->hasNext();
    EXPECT_EQ((ite->next().second)[0].asByteArray(), ByteArray("IRAN"));
    ite->hasNext();
    EXPECT_EQ((ite->next().second)[0].asByteArray(), ByteArray("FRANCE"));
    ite->hasNext();
    EXPECT_EQ((ite->next().second)[0].asByteArray(), ByteArray("VIETNAM"));
    ite->hasNext();
    EXPECT_EQ((ite->next().second)[0].asByteArray(), ByteArray("BRAZIL"));
    ite->hasNext();
    EXPECT_EQ((ite->next().second)[0].asByteArray(), ByteArray("PERU"));
    ite->hasNext();
    EXPECT_EQ((ite->next().second)[0].asByteArray(), ByteArray("JORDAN"));
    ite->hasNext();
    EXPECT_EQ((ite->next().second)[0].asByteArray(), ByteArray("INDONESIA"));
    ite->hasNext();
    EXPECT_EQ((ite->next().second)[0].asByteArray(), ByteArray("UNITED STATES"));
    ite->hasNext();
    EXPECT_EQ((ite->next().second)[0].asByteArray(), ByteArray("ETHIOPIA"));
    ite->hasNext();
    EXPECT_EQ((ite->next().second)[0].asByteArray(), ByteArray("SAUDI ARABIA"));
    ite->hasNext();
    EXPECT_EQ((ite->next().second)[0].asByteArray(), ByteArray("ARGENTINA"));
    ite->hasNext();
    EXPECT_EQ((ite->next().second)[0].asByteArray(), ByteArray("MOZAMBIQUE"));
    ite->hasNext();
    EXPECT_EQ((ite->next().second)[0].asByteArray(), ByteArray("JAPAN"));
    ite->hasNext();
    EXPECT_EQ((ite->next().second)[0].asByteArray(), ByteArray("INDIA"));
    ite->hasNext();
    EXPECT_EQ((ite->next().second)[0].asByteArray(), ByteArray("UNITED KINGDOM"));
    ite->hasNext();
    EXPECT_EQ((ite->next().second)[0].asByteArray(), ByteArray("EGYPT"));
    ite->hasNext();
    EXPECT_EQ((ite->next().second)[0].asByteArray(), ByteArray("ROMANIA"));
    EXPECT_FALSE(ite->hasNext());
}