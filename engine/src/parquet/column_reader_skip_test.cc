//
// Created by Harper on 4/14/20.
//

#include <gtest/gtest.h>
#include "column_reader.h"
#include "file_reader.h"

using namespace parquet;

TEST(ColumnReaderSkipTest, TestSkip) {
    // TODO This testcase is not ready as the Skip does not support data with def level, but this test data has def level.
    auto fileReader = ParquetFileReader::OpenFile("testres/multipage_delta");

    auto row_group_0 = fileReader->RowGroup(0);
    auto column_reader = row_group_0->Column(0);

    int buffer;
    int16_t def;
    int64_t value_read;
    // Case 1: Test Random loading
    column_reader->MoveTo(122);
    column_reader->ReadBatch(1, &def, 0, &buffer, &value_read);
    EXPECT_EQ(buffer, 130);
    // Case 2: Test Whole Page Skipping

    column_reader->MoveTo(300);
    column_reader->ReadBatch(1, &def, 0, &buffer, &value_read);
    EXPECT_EQ(buffer, 130);

    column_reader->MoveTo(600);
    column_reader->ReadBatch(1, &def, 0, &buffer, &value_read);
    EXPECT_EQ(buffer, 130);
}