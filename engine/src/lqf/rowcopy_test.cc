//
// Created by Harper on 6/6/20.
//

#include <gtest/gtest.h>
#include "rowcopy.h"
#include "data_model.h"

using namespace lqf;
using namespace lqf::rowcopy;

TEST(RowCopyTest, Raw) {
    vector<uint32_t> col_offset{0, 1, 2, 3, 4, 5, 6, 7, 8};
    RowCopyFactory f;
    auto copier = f.from(RAW)->to(RAW)
            ->from_layout(col_offset)
            ->to_layout(col_offset)
            ->field(F_REGULAR, 0, 0)
            ->field(F_REGULAR, 1, 1)
            ->field(F_REGULAR, 2, 2)
            ->field(F_REGULAR, 3, 3)
            ->field(F_REGULAR, 4, 4)
            ->build();
    MemDataRow mdFrom(col_offset);
    mdFrom[0] = 135;
    mdFrom[1] = 227;
    mdFrom[2] = 881;
    mdFrom[3] = 990;
    mdFrom[4] = 135;
    MemDataRow mdTo(col_offset);

    (*copier)(mdTo, mdFrom);

    for (uint32_t i = 0; i < 5; ++i) {
        EXPECT_EQ(mdFrom[i].asInt(), mdTo[i].asInt()) << i;
        mdTo[i] = 0;
    }

    RowCopyFactory f2;
    copier = f2.from(RAW)->to(RAW)
            ->from_layout(col_offset)->to_layout(col_offset)
            ->field(F_REGULAR, 0, 3)
            ->field(F_REGULAR, 1, 4)
            ->field(F_REGULAR, 2, 5)
            ->field(F_REGULAR, 3, 6)
            ->field(F_REGULAR, 4, 1)
            ->field(F_REGULAR, 5, 2)
            ->field(F_REGULAR, 6, 0)
            ->field(F_REGULAR, 7, 7)
            ->build();

    (*copier)(mdTo, mdFrom);

    EXPECT_EQ(mdFrom[0].asInt(), mdTo[3].asInt());
    EXPECT_EQ(mdFrom[1].asInt(), mdTo[4].asInt());
    EXPECT_EQ(mdFrom[2].asInt(), mdTo[5].asInt());
    EXPECT_EQ(mdFrom[3].asInt(), mdTo[6].asInt());
    EXPECT_EQ(mdFrom[4].asInt(), mdTo[1].asInt());
    EXPECT_EQ(mdFrom[5].asInt(), mdTo[2].asInt());
    EXPECT_EQ(mdFrom[6].asInt(), mdTo[0].asInt());
    EXPECT_EQ(mdFrom[7].asInt(), mdTo[7].asInt());
}

TEST(RowCopyTest, FieldCopy) {
    vector<uint32_t> col_offset{0, 1, 2, 3, 4, 5, 6, 7, 8};
    RowCopyFactory f;
    auto copier = f.from(OTHER)->to(OTHER)
            ->field(F_REGULAR, 0, 0)
            ->field(F_REGULAR, 1, 1)
            ->field(F_REGULAR, 2, 2)
            ->field(F_REGULAR, 3, 3)
            ->field(F_REGULAR, 4, 4)
            ->build();
    MemDataRow mdFrom(col_offset);
    mdFrom[0] = 135;
    mdFrom[1] = 227;
    mdFrom[2] = 881;
    mdFrom[3] = 990;
    mdFrom[4] = 135;
    MemDataRow mdTo(col_offset);

    (*copier)(mdTo, mdFrom);

    for (uint32_t i = 0; i < 5; ++i) {
        EXPECT_EQ(mdFrom[i].asInt(), mdTo[i].asInt()) << i;
    }

    for (uint32_t i = 0; i < 8; ++i) {
        mdFrom[i] = 0;
    }

    RowCopyFactory f2;
    copier = f2.from(RAW)->to(RAW)
            ->field(F_REGULAR, 0, 2)
            ->field(F_REGULAR, 1, 0)
            ->field(F_REGULAR, 2, 3)
            ->field(F_REGULAR, 3, 1)
            ->field(F_REGULAR, 4, 5)
            ->field(F_REGULAR, 5, 6)
            ->field(F_REGULAR, 6, 4)
            ->field(F_REGULAR, 7, 7)
            ->build();

    (*copier)(mdTo, mdFrom);
    EXPECT_EQ(mdFrom[0].asInt(), mdTo[2].asInt());
    EXPECT_EQ(mdFrom[1].asInt(), mdTo[0].asInt());
    EXPECT_EQ(mdFrom[2].asInt(), mdTo[3].asInt());
    EXPECT_EQ(mdFrom[3].asInt(), mdTo[1].asInt());
    EXPECT_EQ(mdFrom[4].asInt(), mdTo[5].asInt());
    EXPECT_EQ(mdFrom[5].asInt(), mdTo[6].asInt());
    EXPECT_EQ(mdFrom[6].asInt(), mdTo[4].asInt());
    EXPECT_EQ(mdFrom[7].asInt(), mdTo[7].asInt());
}

TEST(RowCopyTest, FieldCopyWithProcessor) {
    vector<uint32_t> col_offset{0, 1, 2, 3, 4, 5, 6, 7, 8};
    RowCopyFactory f;
    auto copier = f.from(OTHER)->to(OTHER)
            ->process([](DataRow &to, DataRow &from) {
                for (auto i = 0u; i < 5; ++i) {
                    to[i] = from[i];
                }
            })
            ->build();
    MemDataRow mdFrom(col_offset);
    mdFrom[0] = 135;
    mdFrom[1] = 227;
    mdFrom[2] = 881;
    mdFrom[3] = 990;
    mdFrom[4] = 135;
    MemDataRow mdTo(col_offset);

    (*copier)(mdTo, mdFrom);

    for (uint32_t i = 0; i < 5; ++i) {
        EXPECT_EQ(mdFrom[i].asInt(), mdTo[i].asInt()) << i;
    }
}


TEST(RowCopyTest, SnapshotTest) {
    vector<uint32_t> col_offset{0, 1, 2, 3, 4, 5, 6, 7, 8};
    RowCopyFactory f;
    auto snapshoter = f.from(RAW)->to(RAW)
            ->from_layout(col_offset)
            ->field(F_REGULAR, 3, 0)
            ->field(F_REGULAR, 4, 1)
            ->field(F_REGULAR, 6, 2)
            ->field(F_REGULAR, 7, 3)
            ->buildSnapshot();
    MemDataRow mdFrom(col_offset);
    mdFrom[0] = 135;
    mdFrom[1] = 227;
    mdFrom[2] = 881;
    mdFrom[3] = 990;
    mdFrom[4] = 135;
    mdFrom[5] = 281;
    mdFrom[6] = 232;
    mdFrom[7] = 777;

    auto snapshot = (*snapshoter)(mdFrom);
    EXPECT_EQ(4, snapshot->size());
    EXPECT_EQ(4, snapshot->num_fields());
    EXPECT_EQ(colOffset(4), snapshot->offset());

    EXPECT_EQ((*snapshot)[0].asInt(), 990);
    EXPECT_EQ((*snapshot)[1].asInt(), 135);
    EXPECT_EQ((*snapshot)[2].asInt(), 232);
    EXPECT_EQ((*snapshot)[3].asInt(), 777);

}

TEST(RowCopyTest, SnapshotTestWithString) {
    vector<uint32_t> col_offset{0, 1, 2, 3, 5, 6};
    RowCopyFactory f;
    auto snapshoter = f.from(RAW)->to(RAW)
            ->from_layout(col_offset)
            ->field(F_STRING, 3, 0)
            ->field(F_REGULAR, 4, 1)
            ->field(F_REGULAR, 1, 2)
            ->field(F_REGULAR, 2, 3)
            ->buildSnapshot();
    MemDataRow mdFrom(col_offset);
    mdFrom[0] = 135;
    mdFrom[1] = 227;
    mdFrom[2] = 881;
    ByteArray ref("23243242");
    mdFrom[3] = ref;
    mdFrom[4] = 941;

    auto snapshot = (*snapshoter)(mdFrom);
    EXPECT_EQ(5, snapshot->size());
    EXPECT_EQ(4, snapshot->num_fields());
    EXPECT_EQ(vector<uint32_t>({0, 2, 3, 4, 5}), snapshot->offset());

    EXPECT_EQ((*snapshot)[0].asByteArray(), ByteArray("23243242"));
    EXPECT_EQ((*snapshot)[1].asInt(), 941);
    EXPECT_EQ((*snapshot)[2].asInt(), 227);
    EXPECT_EQ((*snapshot)[3].asInt(), 881);
}

TEST(RowCopyTest, SnapshotTestWithProcessor) {
    vector<uint32_t> col_offset{0, 1, 2, 3, 5, 6};
    RowCopyFactory f;
    auto snapshoter = f.from(RAW)->to(RAW)
            ->from_layout(vector<uint32_t>({0, 1, 2, 3, 5, 6}))
            ->to_layout(vector<uint32_t>({0, 2, 3, 4, 5}))
            ->process([](DataRow &to, DataRow &from) {
                to[0] = from[3];
                to[1] = from[4];
                to[2] = from[1];
                to[3] = from[2];
            })->buildSnapshot();
    MemDataRow mdFrom(col_offset);
    mdFrom[0] = 135;
    mdFrom[1] = 227;
    mdFrom[2] = 881;
    ByteArray ref("23243242");
    mdFrom[3] = ref;
    mdFrom[4] = 941;

    auto snapshot = (*snapshoter)(mdFrom);
    EXPECT_EQ(5, snapshot->size());
    EXPECT_EQ(4, snapshot->num_fields());
    EXPECT_EQ(vector<uint32_t>({0, 2, 3, 4, 5}), snapshot->offset());

    EXPECT_EQ((*snapshot)[0].asByteArray(), ByteArray("23243242"));
    EXPECT_EQ((*snapshot)[1].asInt(), 941);
    EXPECT_EQ((*snapshot)[2].asInt(), 227);
    EXPECT_EQ((*snapshot)[3].asInt(), 881);
}
