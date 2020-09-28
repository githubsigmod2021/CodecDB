//
// Created by harper on 2/14/20.
//

#include <gtest/gtest.h>
#include "data_model.cc"
#include "rowcopy.h"

using namespace lqf;


TEST(MemBlockTest, Column) {
    MemBlock mb(100, 5);

    vector<double> buf1;
    vector<int32_t> buf2;

    srand(time(NULL));
    for (int i = 0; i < 100; ++i) {
        buf1.push_back(drand48());
        buf2.push_back(rand() % 500);
    }

    auto col0 = mb.col(0);
    auto col1 = mb.col(1);
    for (int i = 0; i < 100; ++i) {
        (*col0)[i] = buf1[i];
        (*col1)[i] = buf2[i];
    }

    auto col0r = mb.col(0);
    auto col1r = mb.col(1);

    for (int i = 0; i < 100; ++i) {
        EXPECT_DOUBLE_EQ(buf1[i], (*col0r)[i].asDouble());
        EXPECT_EQ(1, (*col0r)[i].size_);

        EXPECT_EQ(buf2[i], (*col1r)[i].asInt());
        EXPECT_EQ(1, (*col1r)[i].size_);
    }
}

TEST(MemBlockTest, ColumnString) {
    vector<uint32_t> col_offsets({0, 1, 2, 4});
    MemBlock mb(100, col_offsets);

    char buffer[3000];
    for (int i = 0; i < 3000; ++i) {
        buffer[i] = 'a' + (i % 26);
        if (i != 0 && i % 29 == 0) {
            buffer[i] = 0;
        }
    }

    vector<double> buf1;
    vector<int32_t> buf2;
    vector<ByteArray> buf3;


    srand(time(NULL));
    for (int i = 0; i < 100; ++i) {
        buf1.push_back(drand48());
        buf2.push_back(rand() % 500);
        buf3.push_back(ByteArray(buffer + i * 30));
    }

    auto col0 = mb.col(0);
    auto col1 = mb.col(1);
    auto col2 = mb.col(2);
    for (int i = 0; i < 100; ++i) {
        (*col0)[i] = buf1[i];
        (*col1)[i] = buf2[i];
        (*col2)[i] = buf3[i];
    }

    auto col0r = mb.col(0);
    auto col1r = mb.col(1);
    auto col2r = mb.col(2);

    for (int i = 0; i < 100; ++i) {
        EXPECT_DOUBLE_EQ(buf1[i], (*col0r)[i].asDouble());
        EXPECT_EQ(1, (*col0r)[i].size_);

        EXPECT_EQ(buf2[i], (*col1r)[i].asInt());
        EXPECT_EQ(1, (*col1r)[i].size_);

        EXPECT_EQ(buf3[i], (*col2r)[i].asByteArray());
        EXPECT_TRUE(buf3.data() + i != (*col2r)[i].pointer_.sval_);
        EXPECT_EQ(2, (*col2r)[i].size_);
    }
}

TEST(MemBlockTest, Row) {
    vector<uint32_t> col_offsets({0, 1, 2, 4});
    MemBlock mb(100, col_offsets);

    char buffer[3000];
    for (int i = 0; i < 3000; ++i) {
        buffer[i] = 'a' + (i % 26);
        if (i != 0 && i % 29 == 0) {
            buffer[i] = 0;
        }
    }

    vector<double> buf1;
    vector<int32_t> buf2;
    vector<ByteArray> buf3;


    srand(time(NULL));
    for (int i = 0; i < 100; ++i) {
        buf1.push_back(drand48());
        buf2.push_back(rand() % 500);
        buf3.push_back(ByteArray(buffer + i * 30));
    }


    auto row = mb.rows();

    for (int i = 0; i < 100; ++i) {
        (*row)[i][0] = buf1[i];
        (*row)[i][1] = buf2[i];
        (*row)[i][2] = buf3[i];
    }

    auto roww = mb.rows();
    for (int i = 0; i < 100; ++i) {
        EXPECT_DOUBLE_EQ(buf1[i], (*roww)[i][0].asDouble());
        EXPECT_EQ(1, (*roww)[i][0].size_);

        EXPECT_EQ(buf2[i], (*roww)[i][1].asInt());
        EXPECT_EQ(1, (*roww)[i][1].size_);

        EXPECT_EQ(buf3[i], (*roww)[i][2].asByteArray());
        EXPECT_TRUE(buf3.data() + i != (*roww)[i][2].pointer_.sval_);
        EXPECT_EQ(2, (*roww)[i][2].size_);
    }
}

TEST(MemBlockTest, RowCopy) {
    vector<uint32_t> col_offsets({0, 1, 2, 4, 5});
    MemBlock mb(100, col_offsets);

    char buffer[3000];
    for (int i = 0; i < 3000; ++i) {
        buffer[i] = 'a' + (i % 26);
        if (i != 0 && i % 29 == 0) {
            buffer[i] = 0;
        }
    }

    vector<double> buf1;
    vector<int32_t> buf2;
    vector<ByteArray> buf3;


    srand(time(NULL));
    for (int i = 0; i < 100; ++i) {
        buf1.push_back(drand48());
        buf2.push_back(rand() % 500);
        buf3.push_back(ByteArray(buffer + i * 30));
    }

    MemDataRow mdr(col_offsets);

    auto write_row = mb.rows();
    for (int i = 0; i < 100; ++i) {
        mdr[0] = buf1[i];
        mdr[1] = buf2[i];
        mdr[2] = buf3[i];
        (*write_row)[i] = mdr;
    }

    auto roww = mb.rows();
    for (int i = 0; i < 100; ++i) {
        EXPECT_DOUBLE_EQ(buf1[i], (*roww)[i][0].asDouble());
        EXPECT_EQ(1, (*roww)[i][0].size_);

        EXPECT_EQ(buf2[i], (*roww)[i][1].asInt());
        EXPECT_EQ(1, (*roww)[i][1].size_);

        EXPECT_EQ(buf3[i], (*roww)[i][2].asByteArray());
        EXPECT_TRUE(buf3.data() + i != (*roww)[i][2].pointer_.sval_);
        EXPECT_EQ(2, (*roww)[i][2].size_);
    }
}

TEST(MemBlockTest, Mask) {
    shared_ptr<MemBlock> mbp = make_shared<MemBlock>(100, 5);
    auto roww = mbp->rows();
    for (int i = 0; i < 100; ++i) {
        (*roww)[i][0] = i;
    }
    shared_ptr<Bitmap> bitmap = make_shared<SimpleBitmap>(100);
    bitmap->put(4);
    bitmap->put(20);
    bitmap->put(95);
    auto masked = mbp->mask(bitmap);

    EXPECT_EQ(3, masked->size());
    auto rows = masked->rows();

    EXPECT_EQ(4, rows->next()[0].asInt());
    EXPECT_EQ(20, rows->next()[0].asInt());
    EXPECT_EQ(95, rows->next()[0].asInt());
}

TEST(MemvBlockTest, Column) {
    vector<uint32_t> col_offsets({0, 1, 2, 4});
    vector<uint32_t> col_size({1, 1, 2});
    MemvBlock mb(100, col_size);

    char buffer[3000];
    for (int i = 0; i < 3000; ++i) {
        buffer[i] = 'a' + (i % 26);
        if (i != 0 && i % 29 == 0) {
            buffer[i] = 0;
        }
    }

    vector<double> buf1;
    vector<int32_t> buf2;
    vector<ByteArray> buf3;


    srand(time(NULL));
    for (int i = 0; i < 100; ++i) {
        buf1.push_back(drand48());
        buf2.push_back(rand() % 500);
        buf3.push_back(ByteArray(buffer + i * 30));
    }

    auto col0 = mb.col(0);
    auto col1 = mb.col(1);
    auto col2 = mb.col(2);
    for (int i = 0; i < 100; ++i) {
        (*col0)[i] = buf1[i];
        (*col1)[i] = buf2[i];
        (*col2)[i] = buf3[i];
    }

    auto col0r = mb.col(0);
    auto col1r = mb.col(1);
    auto col2r = mb.col(2);

    for (int i = 0; i < 100; ++i) {
        EXPECT_DOUBLE_EQ(buf1[i], (*col0r)[i].asDouble());
        EXPECT_EQ(1, (*col0r)[i].size_);

        EXPECT_EQ(buf2[i], (*col1r)[i].asInt());
        EXPECT_EQ(1, (*col1r)[i].size_);

        EXPECT_EQ(buf3[i], (*col2r)[i].asByteArray());
        EXPECT_TRUE(buf3.data() + i != (*col2r)[i].pointer_.sval_);
        EXPECT_EQ(2, (*col2r)[i].size_);
    }
}

TEST(MemvBlockTest, Row) {
    vector<uint32_t> col_size({1, 1, 2});
    MemvBlock mb(100, col_size);

    char buffer[3000];
    for (int i = 0; i < 3000; ++i) {
        buffer[i] = 'a' + (i % 26);
        if (i != 0 && i % 29 == 0) {
            buffer[i] = 0;
        }
    }

    vector<double> buf1;
    vector<int32_t> buf2;
    vector<ByteArray> buf3;


    srand(time(NULL));
    for (int i = 0; i < 100; ++i) {
        buf1.push_back(drand48());
        buf2.push_back(rand() % 500);
        buf3.push_back(ByteArray(buffer + i * 30));
    }


    auto row = mb.rows();

    for (int i = 0; i < 100; ++i) {
        (*row)[i][0] = buf1[i];
        (*row)[i][1] = buf2[i];
        (*row)[i][2] = buf3[i];
    }

    auto roww = mb.rows();
    for (int i = 0; i < 100; ++i) {
        EXPECT_DOUBLE_EQ(buf1[i], (*roww)[i][0].asDouble());
        EXPECT_EQ(1, (*roww)[i][0].size_);

        EXPECT_EQ(buf2[i], (*roww)[i][1].asInt());
        EXPECT_EQ(1, (*roww)[i][1].size_);

        EXPECT_EQ(buf3[i], (*roww)[i][2].asByteArray());
        EXPECT_TRUE(buf3.data() + i != (*roww)[i][2].pointer_.sval_);
        EXPECT_EQ(2, (*roww)[i][2].size_);
    }
}

TEST(MemvBlockTest, RowCopy) {
    char buffer[3000];
    for (int i = 0; i < 3000; ++i) {
        buffer[i] = 'a' + (i % 26);
        if (i != 0 && i % 29 == 0) {
            buffer[i] = 0;
        }
    }

    vector<double> buf1;
    vector<int32_t> buf2;
    vector<ByteArray> buf3;

    srand(time(NULL));
    for (int i = 0; i < 100; ++i) {
        buf1.push_back(drand48());
        buf2.push_back(rand() % 500);
        buf3.push_back(ByteArray(buffer + i * 30));
    }

    vector<uint32_t> col_size({1, 1, 2});
    vector<uint32_t> col_offset({0, 1, 2, 4});

    MemvBlock mb(100, col_size);
    MemDataRow mdr(col_offset);

    auto write_row = mb.rows();
    for (int i = 0; i < 100; ++i) {
        mdr[0] = buf1[i];
        mdr[1] = buf2[i];
        mdr[2] = buf3[i];
        (*write_row)[i] = mdr;
    }


    auto roww = mb.rows();
    for (int i = 0; i < 100; ++i) {
        EXPECT_DOUBLE_EQ(buf1[i], (*roww)[i][0].asDouble());
        EXPECT_EQ(1, (*roww)[i][0].size_);

        EXPECT_EQ(buf2[i], (*roww)[i][1].asInt());
        EXPECT_EQ(1, (*roww)[i][1].size_);

        EXPECT_EQ(buf3[i], (*roww)[i][2].asByteArray());
        EXPECT_TRUE(buf3.data() + i != (*roww)[i][2].pointer_.sval_);
        EXPECT_EQ(2, (*roww)[i][2].size_);
    }
}

TEST(FlexBlockTest, Access) {

    MemFlexBlock flex(colOffset(3));
    for (int i = 0; i < 100; ++i) {
        DataRow &writeto = flex.push_back();
        writeto[0] = i;
        writeto[1] = i * 2 + 0.1;
        writeto[2] = i * 3;
    }
    EXPECT_EQ(100, flex.size());
    auto rows = flex.rows();
    for (int i = 0; i < 100; ++i) {
        DataRow &row = rows->next();
        EXPECT_EQ(row[0].asInt(), i);
        EXPECT_EQ(row[1].asDouble(), i * 2 + 0.1);
        EXPECT_EQ(row[2].asInt(), i * 3);
    }

}


class ParquetBlockTest : public ::testing::Test {
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

TEST_F(ParquetBlockTest, Column) {
    auto block = make_shared<ParquetBlock>(nullptr, rowGroup_, 0, 3);
    auto col = block->col(1);
//    std::vector<int32_t> buffer;
//    for(int i = 0 ;i<100;++i) {
//        buffer.push_back((*col)[i].asInt());
//    }
    EXPECT_EQ(156, (*col)[0].asInt());
    EXPECT_EQ(1, (*col)[0].size_);
    EXPECT_EQ(68, (*col)[1].asInt());
    EXPECT_EQ(1, (*col)[1].size_);
    EXPECT_EQ(64, (*col)[2].asInt());
    EXPECT_EQ(1, (*col)[2].size_);
//    EXPECT_EQ(3, (*col)[3].asInt());
//    EXPECT_EQ(25, (*col)[4].asInt());
//    EXPECT_EQ(16, (*col)[5].asInt());
//    EXPECT_EQ(107, (*col)[6].asInt());
//    EXPECT_EQ(5, (*col)[7].asInt());
//    EXPECT_EQ(20, (*col)[8].asInt());
    EXPECT_EQ(129, (*col)[9].asInt());
    EXPECT_EQ(30, (*col)[10].asInt());
//    EXPECT_EQ(184, (*col)[11].asInt());
    EXPECT_EQ(63, (*col)[12].asInt());
//    EXPECT_EQ(89, (*col)[13].asInt());
//    EXPECT_EQ(109, (*col)[14].asInt());
    EXPECT_EQ(21, (*col)[52].asInt());
    EXPECT_EQ(55, (*col)[53].asInt());
    EXPECT_EQ(95, (*col)[54].asInt());

    auto col2 = block->col(1);
    EXPECT_EQ(156, col2->next().asInt());
    EXPECT_EQ(68, col2->next().asInt());
    EXPECT_EQ(64, col2->next().asInt());
}

class IntDictScanner : public Int32Accessor {
protected:
    Dictionary<Int32Type> dict_;
public:
    void dict(Dictionary<PhysicalType<parquet::Type::INT32> > &dict) override {
        // Do nothing just maintain a dict
        this->dict_ = move(dict);
    }

    void scanPage(uint64_t numEntry, const uint8_t *data, uint64_t *bitmap, uint64_t bitmap_offset) override {
        // Do nothing just access the page
    }

    Int32Dictionary *accessDict() {
        return &this->dict_;
    }
};

class StringDictScanner : public ByteArrayAccessor {
protected:
    Dictionary<ByteArrayType> dict_;
public:
    void dict(Dictionary<ByteArrayType> &dict) override {
        // Do nothing just maintain a dict
        this->dict_ = move(dict);
    }

    void scanPage(uint64_t numEntry, const uint8_t *data, uint64_t *bitmap, uint64_t bitmap_offset) override {
        // Do nothing just access the page
    }

    ByteArrayDictionary *accessDict() {
        return &this->dict_;
    }
};


TEST_F(ParquetBlockTest, Raw) {
    auto block = make_shared<ParquetBlock>(nullptr, rowGroup_, 0, 3);
    auto scanner = new IntDictScanner();
    block->raw(1, scanner);
    auto dict = scanner->accessDict();
    EXPECT_EQ(200, dict->size());

    EXPECT_EQ(-201, dict->lookup(375));
    EXPECT_EQ(102, dict->lookup(103));
    EXPECT_EQ(-201, dict->lookup(514));
    EXPECT_EQ(18, dict->lookup(19));

    delete scanner;

    auto scanner2 = new StringDictScanner();
    block->raw(10, scanner2);
    auto dict2 = scanner2->accessDict();

    EXPECT_EQ(2266, dict2->size());
    EXPECT_EQ(675, dict2->lookup(ByteArray("1994-02-01")));
    delete scanner2;
}

TEST_F(ParquetBlockTest, Row) {
    auto block = make_shared<ParquetBlock>(nullptr, rowGroup_, 0, (1 << 14) - 1);
    auto rows = block->rows();

    // Test reading and repeatable read
    EXPECT_EQ(1, (*rows)[2][0].asInt());
    EXPECT_EQ(1, (*rows)[2][0].asInt());
    EXPECT_EQ(25, (*rows)[4][1].asInt());
    EXPECT_EQ(25, (*rows)[4][1].asInt());
    EXPECT_EQ(ByteArray("NONE"), (*rows)[4][13].asByteArray());
    EXPECT_EQ(2, (*rows)[4][13].size_);
    EXPECT_EQ(ByteArray("DELIVER IN PERSON"), (*rows)[5][13].asByteArray());
    EXPECT_EQ(2, (*rows)[5][13].size_);

    auto rows2 = block->rows();
    DataRow &row = rows2->next();
    EXPECT_EQ(1, row[0].asInt());
    EXPECT_EQ(1, row[0].asInt());
    EXPECT_EQ(156, row[1].asInt());
    EXPECT_EQ(156, row[1].asInt());
    EXPECT_EQ(4, row[2].asInt());
    EXPECT_EQ(4, row[2].asInt());
    row = rows2->next();
    EXPECT_EQ(1, row[0].asInt());
    EXPECT_EQ(68, row[1].asInt());
    EXPECT_EQ(9, row[2].asInt());

//    try {
//        row[3].asInt();
//        FAIL() << "Should not reach here";
//    } catch (const std::invalid_argument &ia) {
//
//    }
}

TEST_F(ParquetBlockTest, Mask) {
    auto block = make_shared<ParquetBlock>(nullptr, rowGroup_, 0, 3);

    shared_ptr<Bitmap> bitmap = make_shared<SimpleBitmap>(100);
    bitmap->put(4);
    bitmap->put(20);
    bitmap->put(95);
    auto masked = block->mask(bitmap);

    EXPECT_EQ(3, masked->size());
    auto rows = masked->rows();
    EXPECT_EQ(1, (*rows)[0][0].asInt());
    EXPECT_EQ(0, rows->pos());
    EXPECT_EQ(3, (*rows)[10][0].asInt());
    EXPECT_EQ(10, rows->pos());
    EXPECT_EQ(32, (*rows)[25][0].asInt());
    EXPECT_EQ(25, rows->pos());

    auto row2 = masked->rows();
    EXPECT_EQ(1, row2->next()[0].asInt());
    EXPECT_EQ(4, row2->pos());
    EXPECT_EQ(7, row2->next()[0].asInt());
    EXPECT_EQ(20, row2->pos());
    EXPECT_EQ(97, row2->next()[0].asInt());
    EXPECT_EQ(95, row2->pos());
}

TEST_F(ParquetBlockTest, MaskOnMask) {
    auto block = make_shared<ParquetBlock>(nullptr, rowGroup_, 0, 3);

    shared_ptr<Bitmap> bitmap = make_shared<SimpleBitmap>(200);
    bitmap->put(4);
    bitmap->put(20);
    bitmap->put(95);
    bitmap->put(128);
    bitmap->put(37);

    auto masked = block->mask(bitmap);

    shared_ptr<Bitmap> bitmap2 = make_shared<SimpleBitmap>(200);
    bitmap2->put(20);
    bitmap2->put(95);

    auto masked2 = masked->mask(bitmap2);

    EXPECT_EQ(2, masked->size());
    auto rows = masked->rows();
    EXPECT_EQ(1, (*rows)[0][0].asInt());
    EXPECT_EQ(3, (*rows)[10][0].asInt());
    EXPECT_EQ(32, (*rows)[25][0].asInt());

    auto row2 = masked->rows();
    EXPECT_EQ(7, row2->next()[0].asInt());
    EXPECT_EQ(97, row2->next()[0].asInt());
}

class RawDataAccessorForTest : public Int32Accessor {
public:
    int dict_value_;
    uint64_t pos_ = 0;

    void dict(Int32Dictionary &dict) override {
        int a = 102;
        dict_value_ = dict.lookup(a);
    }

    void scanPage(uint64_t numEntry, const uint8_t *data, uint64_t *bitmap, uint64_t bitmap_offset) override {
//        auto dpv1 = static_cast<DataPageV1 *>(page);

        pos_ += numEntry;
    }
};

using namespace parquet;

TEST_F(ParquetBlockTest, RawAndDict) {
    auto block = make_shared<ParquetBlock>(nullptr, rowGroup_, 0, 3);
    shared_ptr<RawDataAccessorForTest> accessor = make_shared<RawDataAccessorForTest>();
    auto bitmap = block->raw(0, accessor.get());

    EXPECT_EQ(0, bitmap->cardinality());
    EXPECT_EQ(29, accessor->dict_value_);
}

TEST(ParquetTableTest, LoadDictionary) {
    auto ptable = ParquetTable::Open("testres/lineitem", (1 << 15) - 1);
    auto dictionary = ptable->LoadDictionary<parquet::ByteArrayType>(8);

    EXPECT_EQ(dictionary->size(), 3);
    EXPECT_EQ((*dictionary)[0], ByteArray("A"));
    EXPECT_EQ((*dictionary)[1], ByteArray("N"));
    EXPECT_EQ((*dictionary)[2], ByteArray("R"));

    auto dictionary2 = ptable->LoadDictionary<parquet::ByteArrayType>(9);

    EXPECT_EQ(dictionary2->size(), 2);
    EXPECT_EQ((*dictionary2)[0], ByteArray("F"));
    EXPECT_EQ((*dictionary2)[1], ByteArray("O"));
}

TEST(MaskedTableTest, Create) {
    auto ptable = ParquetTable::Open("testres/lineitem", 0x7);

    vector<shared_ptr<Bitmap>> masks(10, nullptr);
    masks[0] = make_shared<SimpleBitmap>(1000);
    auto maskTable = make_shared<MaskedTable>(ptable.get(), masks);

    auto round1 = maskTable->blocks()->collect();
    auto round2 = maskTable->blocks()->collect();

    EXPECT_EQ(dynamic_pointer_cast<MaskedBlock>((*round1)[0])->mask()->size(), 1000);
    EXPECT_EQ(dynamic_pointer_cast<MaskedBlock>((*round2)[0])->mask()->size(), 1000);
}

TEST(MemTableTest, Create) {
    auto mt = MemTable::Make(5);

    mt->allocate(100);
    mt->allocate(200);
    mt->allocate(300);

    auto stream = mt->blocks();

    auto list = *(stream->collect());

    ASSERT_EQ(3, list.size());
    ASSERT_EQ(100, list[0]->size());
    ASSERT_EQ(200, list[1]->size());
    ASSERT_EQ(300, list[2]->size());
}

TEST(TableViewTest, Create) {
    auto parquetTable = ParquetTable::Open("testres/lineitem");
    auto blocks = parquetTable->blocks();
    TableView tableView(parquetTable->type(), parquetTable->colSize(), move(blocks));

    tableView.blocks()->foreach([](const shared_ptr<Block> &block) { cout << block->size() << endl; });
//    tableView.blocks()->foreach([](const shared_ptr<Block> &block) { cout << block->size() << endl; });
}

TEST(DataRowTest, Copy) {

    auto table = MemTable::Make(4);
    auto block = table->allocate(100);
    auto block2 = table->allocate(100);

    vector<MemDataRow> buffer;

    auto rows1 = block->rows();
    auto rows2 = block2->rows();

    srand(time(NULL));

    using namespace rowcopy;
    auto copier = RowCopyFactory().buildAssign(RAW, RAW, 4);

    for (int i = 0; i < 100; ++i) {
        (*rows1)[i][0] = rand();
        buffer.emplace_back(4);
        (*copier)(buffer[i], (*rows1)[i]);
        EXPECT_EQ((*rows1)[i][0].asInt(), buffer[i][0].asInt());
        (*rows2)[i] = buffer[i];
        EXPECT_EQ((*rows1)[i][0].asInt(), (*rows2)[i][0].asInt());
    }
}

TEST(DataRowCopyTest, Copy) {
}