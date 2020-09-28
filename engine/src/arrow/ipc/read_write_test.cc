// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <ostream>
#include <string>

#include <flatbuffers/flatbuffers.h>
#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/io/file.h"
#include "arrow/io/memory.h"
#include "arrow/io/test_common.h"
#include "arrow/ipc/message.h"
#include "arrow/ipc/metadata_internal.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/test_common.h"
#include "arrow/ipc/writer.h"
#include "arrow/memory_pool.h"
#include "arrow/record_batch.h"
#include "arrow/sparse_tensor.h"
#include "arrow/status.h"
#include "arrow/tensor.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/key_value_metadata.h"

#include "generated/Message_generated.h"  // IWYU pragma: keep

namespace arrow {

using internal::checked_cast;

namespace ipc {
namespace test {

using BatchVector = std::vector<std::shared_ptr<RecordBatch>>;

TEST(TestMessage, Equals) {
  std::string metadata = "foo";
  std::string body = "bar";

  auto b1 = std::make_shared<Buffer>(metadata);
  auto b2 = std::make_shared<Buffer>(metadata);
  auto b3 = std::make_shared<Buffer>(body);
  auto b4 = std::make_shared<Buffer>(body);

  Message msg1(b1, b3);
  Message msg2(b2, b4);
  Message msg3(b1, nullptr);
  Message msg4(b2, nullptr);

  ASSERT_TRUE(msg1.Equals(msg2));
  ASSERT_TRUE(msg3.Equals(msg4));

  ASSERT_FALSE(msg1.Equals(msg3));
  ASSERT_FALSE(msg3.Equals(msg1));

  // same metadata as msg1, different body
  Message msg5(b2, b1);
  ASSERT_FALSE(msg1.Equals(msg5));
  ASSERT_FALSE(msg5.Equals(msg1));
}

TEST(TestMessage, SerializeTo) {
  const int64_t body_length = 64;

  flatbuffers::FlatBufferBuilder fbb;
  fbb.Finish(flatbuf::CreateMessage(fbb, internal::kCurrentMetadataVersion,
                                    flatbuf::MessageHeader::RecordBatch, 0 /* header */,
                                    body_length));

  std::shared_ptr<Buffer> metadata;
  ASSERT_OK_AND_ASSIGN(metadata, internal::WriteFlatbufferBuilder(fbb));

  std::string body = "abcdef";

  std::unique_ptr<Message> message;
  ASSERT_OK(Message::Open(metadata, std::make_shared<Buffer>(body), &message));

  auto CheckWithAlignment = [&](int32_t alignment) {
    IpcOptions options;
    options.alignment = alignment;
    const int32_t prefix_size = 8;
    int64_t output_length = 0;
    ASSERT_OK_AND_ASSIGN(auto stream, io::BufferOutputStream::Create(1 << 10));
    ASSERT_OK(message->SerializeTo(stream.get(), options, &output_length));
    ASSERT_EQ(BitUtil::RoundUp(metadata->size() + prefix_size, alignment) + body_length,
              output_length);
    ASSERT_OK_AND_EQ(output_length, stream->Tell());
  };

  CheckWithAlignment(8);
  CheckWithAlignment(64);
}

void BuffersOverlapEquals(const Buffer& left, const Buffer& right) {
  ASSERT_GT(left.size(), 0);
  ASSERT_GT(right.size(), 0);
  ASSERT_TRUE(left.Equals(right, std::min(left.size(), right.size())));
}

TEST(TestMessage, LegacyIpcBackwardsCompatibility) {
  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK(MakeIntBatchSized(36, &batch));

  auto RoundtripWithOptions = [&](const IpcOptions& arg_options,
                                  std::shared_ptr<Buffer>* out_serialized,
                                  std::unique_ptr<Message>* out) {
    internal::IpcPayload payload;
    ASSERT_OK(internal::GetRecordBatchPayload(*batch, arg_options, default_memory_pool(),
                                              &payload));

    ASSERT_OK_AND_ASSIGN(auto stream, io::BufferOutputStream::Create(1 << 20));

    int32_t metadata_length = -1;
    ASSERT_OK(
        internal::WriteIpcPayload(payload, arg_options, stream.get(), &metadata_length));

    ASSERT_OK_AND_ASSIGN(*out_serialized, stream->Finish());
    io::BufferReader io_reader(*out_serialized);
    ASSERT_OK(ReadMessage(&io_reader, out));
  };

  std::shared_ptr<Buffer> serialized, legacy_serialized;
  std::unique_ptr<Message> message, legacy_message;

  IpcOptions options;
  RoundtripWithOptions(options, &serialized, &message);

  // First 4 bytes 0xFFFFFFFF Continuation marker
  ASSERT_EQ(-1, util::SafeLoadAs<int32_t>(serialized->data()));

  options.write_legacy_ipc_format = true;
  RoundtripWithOptions(options, &legacy_serialized, &legacy_message);

  // Check that the continuation marker is not written
  ASSERT_NE(-1, util::SafeLoadAs<int32_t>(legacy_serialized->data()));

  // Have to use the smaller size to exclude padding
  BuffersOverlapEquals(*legacy_message->metadata(), *message->metadata());
  ASSERT_TRUE(legacy_message->body()->Equals(*message->body()));
}

TEST(TestMessage, Verify) {
  std::string metadata = "invalid";
  std::string body = "abcdef";

  Message message(std::make_shared<Buffer>(metadata), std::make_shared<Buffer>(body));
  ASSERT_FALSE(message.Verify());
}

class TestSchemaMetadata : public ::testing::Test {
 public:
  void SetUp() {}

  void CheckRoundtrip(const Schema& schema) {
    std::shared_ptr<Buffer> buffer;
    DictionaryMemo in_memo, out_memo;
    ASSERT_OK(SerializeSchema(schema, &out_memo, default_memory_pool(), &buffer));

    std::shared_ptr<Schema> result;
    io::BufferReader reader(buffer);
    ASSERT_OK(ReadSchema(&reader, &in_memo, &result));
    AssertSchemaEqual(schema, *result);
  }
};

const std::shared_ptr<DataType> INT32 = std::make_shared<Int32Type>();

TEST_F(TestSchemaMetadata, PrimitiveFields) {
  auto f0 = field("f0", std::make_shared<Int8Type>());
  auto f1 = field("f1", std::make_shared<Int16Type>(), false);
  auto f2 = field("f2", std::make_shared<Int32Type>());
  auto f3 = field("f3", std::make_shared<Int64Type>());
  auto f4 = field("f4", std::make_shared<UInt8Type>());
  auto f5 = field("f5", std::make_shared<UInt16Type>());
  auto f6 = field("f6", std::make_shared<UInt32Type>());
  auto f7 = field("f7", std::make_shared<UInt64Type>());
  auto f8 = field("f8", std::make_shared<FloatType>());
  auto f9 = field("f9", std::make_shared<DoubleType>(), false);
  auto f10 = field("f10", std::make_shared<BooleanType>());

  Schema schema({f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10});
  CheckRoundtrip(schema);
}

TEST_F(TestSchemaMetadata, NestedFields) {
  auto type = list(int32());
  auto f0 = field("f0", type);

  std::shared_ptr<StructType> type2(
      new StructType({field("k1", INT32), field("k2", INT32), field("k3", INT32)}));
  auto f1 = field("f1", type2);

  Schema schema({f0, f1});
  CheckRoundtrip(schema);
}

TEST_F(TestSchemaMetadata, DictionaryFields) {
  {
    auto dict_type = dictionary(int8(), int32(), true /* ordered */);
    auto f0 = field("f0", dict_type);
    auto f1 = field("f1", list(dict_type));

    Schema schema({f0, f1});
    CheckRoundtrip(schema);
  }
  {
    auto dict_type = dictionary(int8(), list(int32()));
    auto f0 = field("f0", dict_type);

    Schema schema({f0});
    CheckRoundtrip(schema);
  }
}

TEST_F(TestSchemaMetadata, KeyValueMetadata) {
  auto field_metadata = key_value_metadata({{"key", "value"}});
  auto schema_metadata = key_value_metadata({{"foo", "bar"}, {"bizz", "buzz"}});

  auto f0 = field("f0", std::make_shared<Int8Type>());
  auto f1 = field("f1", std::make_shared<Int16Type>(), false, field_metadata);

  Schema schema({f0, f1}, schema_metadata);
  CheckRoundtrip(schema);
}

#define BATCH_CASES()                                                                   \
  ::testing::Values(&MakeIntRecordBatch, &MakeListRecordBatch, &MakeNonNullRecordBatch, \
                    &MakeZeroLengthRecordBatch, &MakeDeeplyNestedList,                  \
                    &MakeStringTypesRecordBatchWithNulls, &MakeStruct, &MakeUnion,      \
                    &MakeDictionary, &MakeDates, &MakeTimestamps, &MakeTimes,           \
                    &MakeFWBinary, &MakeNull, &MakeDecimal, &MakeBooleanBatch,          \
                    &MakeIntervals)

static int g_file_number = 0;

class IpcTestFixture : public io::MemoryMapFixture {
 public:
  void SetUp() {
    pool_ = default_memory_pool();
    options_ = IpcOptions::Defaults();
  }

  void DoSchemaRoundTrip(const Schema& schema, DictionaryMemo* out_memo,
                         std::shared_ptr<Schema>* result) {
    std::shared_ptr<Buffer> serialized_schema;
    ASSERT_OK(SerializeSchema(schema, out_memo, pool_, &serialized_schema));

    DictionaryMemo in_memo;
    io::BufferReader buf_reader(serialized_schema);
    ASSERT_OK(ReadSchema(&buf_reader, &in_memo, result));
    ASSERT_EQ(out_memo->num_fields(), in_memo.num_fields());
  }

  Status DoStandardRoundTrip(const RecordBatch& batch, DictionaryMemo* dictionary_memo,
                             std::shared_ptr<RecordBatch>* batch_result) {
    std::shared_ptr<Buffer> serialized_batch;
    RETURN_NOT_OK(SerializeRecordBatch(batch, pool_, &serialized_batch));

    io::BufferReader buf_reader(serialized_batch);
    return ReadRecordBatch(batch.schema(), dictionary_memo, &buf_reader, batch_result);
  }

  Status DoLargeRoundTrip(const RecordBatch& batch, bool zero_data,
                          std::shared_ptr<RecordBatch>* result) {
    if (zero_data) {
      RETURN_NOT_OK(ZeroMemoryMap(mmap_.get()));
    }
    RETURN_NOT_OK(mmap_->Seek(0));

    auto options = options_;
    options.allow_64bit = true;

    auto res = RecordBatchFileWriter::Open(mmap_.get(), batch.schema(), options);
    RETURN_NOT_OK(res.status());
    std::shared_ptr<RecordBatchWriter> file_writer = *res;
    RETURN_NOT_OK(file_writer->WriteRecordBatch(batch));
    RETURN_NOT_OK(file_writer->Close());

    ARROW_ASSIGN_OR_RAISE(int64_t offset, mmap_->Tell());

    std::shared_ptr<RecordBatchFileReader> file_reader;
    RETURN_NOT_OK(RecordBatchFileReader::Open(mmap_.get(), offset, &file_reader));

    return file_reader->ReadRecordBatch(0, result);
  }

  void CheckReadResult(const RecordBatch& result, const RecordBatch& expected) {
    ASSERT_OK(result.ValidateFull());
    EXPECT_EQ(expected.num_rows(), result.num_rows());

    ASSERT_TRUE(expected.schema()->Equals(*result.schema()));
    ASSERT_EQ(expected.num_columns(), result.num_columns())
        << expected.schema()->ToString() << " result: " << result.schema()->ToString();

    CompareBatchColumnsDetailed(result, expected);
  }

  void CheckRoundtrip(const RecordBatch& batch, int64_t buffer_size) {
    std::stringstream ss;
    ss << "test-write-row-batch-" << g_file_number++;
    ASSERT_OK_AND_ASSIGN(mmap_,
                         io::MemoryMapFixture::InitMemoryMap(buffer_size, ss.str()));

    DictionaryMemo dictionary_memo;

    std::shared_ptr<Schema> schema_result;
    DoSchemaRoundTrip(*batch.schema(), &dictionary_memo, &schema_result);
    ASSERT_TRUE(batch.schema()->Equals(*schema_result));

    ASSERT_OK(CollectDictionaries(batch, &dictionary_memo));

    std::shared_ptr<RecordBatch> result;
    ASSERT_OK(DoStandardRoundTrip(batch, &dictionary_memo, &result));
    CheckReadResult(*result, batch);

    ASSERT_OK(DoLargeRoundTrip(batch, /*zero_data=*/true, &result));
    CheckReadResult(*result, batch);
  }

  void CheckRoundtrip(const std::shared_ptr<Array>& array, int64_t buffer_size) {
    auto f0 = arrow::field("f0", array->type());
    std::vector<std::shared_ptr<Field>> fields = {f0};
    auto schema = std::make_shared<Schema>(fields);

    auto batch = RecordBatch::Make(schema, 0, {array});
    CheckRoundtrip(*batch, buffer_size);
  }

 protected:
  std::shared_ptr<io::MemoryMappedFile> mmap_;
  MemoryPool* pool_;
  IpcOptions options_;
};

class TestWriteRecordBatch : public ::testing::Test, public IpcTestFixture {
 public:
  void SetUp() { IpcTestFixture::SetUp(); }
  void TearDown() { IpcTestFixture::TearDown(); }
};

class TestIpcRoundTrip : public ::testing::TestWithParam<MakeRecordBatch*>,
                         public IpcTestFixture {
 public:
  void SetUp() { IpcTestFixture::SetUp(); }
  void TearDown() { IpcTestFixture::TearDown(); }
};

TEST_P(TestIpcRoundTrip, RoundTrip) {
  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK((*GetParam())(&batch));  // NOLINT clang-tidy gtest issue

  CheckRoundtrip(*batch, 1 << 20);
}

TEST_F(TestIpcRoundTrip, MetadataVersion) {
  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK(MakeIntRecordBatch(&batch));

  ASSERT_OK_AND_ASSIGN(mmap_,
                       io::MemoryMapFixture::InitMemoryMap(1 << 16, "test-metadata"));

  int32_t metadata_length;
  int64_t body_length;

  const int64_t buffer_offset = 0;

  ASSERT_OK(WriteRecordBatch(*batch, buffer_offset, mmap_.get(), &metadata_length,
                             &body_length, options_, pool_));

  std::unique_ptr<Message> message;
  ASSERT_OK(ReadMessage(0, metadata_length, mmap_.get(), &message));

  ASSERT_EQ(MetadataVersion::V4, message->metadata_version());
}

TEST(TestReadMessage, CorruptedSmallInput) {
  std::string data = "abc";
  io::BufferReader reader(data);
  std::unique_ptr<Message> message;
  ASSERT_RAISES(Invalid, ReadMessage(&reader, &message));

  // But no error on unsignaled EOS
  io::BufferReader reader2("");
  ASSERT_OK(ReadMessage(&reader2, &message));
  ASSERT_EQ(nullptr, message);
}

TEST_P(TestIpcRoundTrip, SliceRoundTrip) {
  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK((*GetParam())(&batch));  // NOLINT clang-tidy gtest issue

  // Skip the zero-length case
  if (batch->num_rows() < 2) {
    return;
  }

  auto sliced_batch = batch->Slice(2, 10);
  CheckRoundtrip(*sliced_batch, 1 << 20);
}

TEST_P(TestIpcRoundTrip, ZeroLengthArrays) {
  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK((*GetParam())(&batch));  // NOLINT clang-tidy gtest issue

  std::shared_ptr<RecordBatch> zero_length_batch;
  if (batch->num_rows() > 2) {
    zero_length_batch = batch->Slice(2, 0);
  } else {
    zero_length_batch = batch->Slice(0, 0);
  }

  CheckRoundtrip(*zero_length_batch, 1 << 20);

  // ARROW-544: check binary array
  std::shared_ptr<Buffer> value_offsets;
  ASSERT_OK(AllocateBuffer(pool_, sizeof(int32_t), &value_offsets));
  *reinterpret_cast<int32_t*>(value_offsets->mutable_data()) = 0;

  std::shared_ptr<Array> bin_array = std::make_shared<BinaryArray>(
      0, value_offsets, std::make_shared<Buffer>(nullptr, 0),
      std::make_shared<Buffer>(nullptr, 0));

  // null value_offsets
  std::shared_ptr<Array> bin_array2 = std::make_shared<BinaryArray>(0, nullptr, nullptr);

  CheckRoundtrip(bin_array, 1 << 20);
  CheckRoundtrip(bin_array2, 1 << 20);
}

TEST_F(TestWriteRecordBatch, SliceTruncatesBinaryOffsets) {
  // ARROW-6046
  std::shared_ptr<Array> array;
  ASSERT_OK(MakeRandomStringArray(500, false, default_memory_pool(), &array));

  auto f0 = field("f0", array->type());
  auto schema = ::arrow::schema({f0});
  auto batch = RecordBatch::Make(schema, array->length(), {array});
  auto sliced_batch = batch->Slice(0, 5);

  std::stringstream ss;
  ss << "test-truncate-offsets";
  ASSERT_OK_AND_ASSIGN(
      mmap_, io::MemoryMapFixture::InitMemoryMap(/*buffer_size=*/1 << 20, ss.str()));
  DictionaryMemo dictionary_memo;
  std::shared_ptr<RecordBatch> result;
  ASSERT_OK(DoStandardRoundTrip(*sliced_batch, &dictionary_memo, &result));
  ASSERT_EQ(6 * sizeof(int32_t), result->column(0)->data()->buffers[1]->size());
}

TEST_F(TestWriteRecordBatch, SliceTruncatesBuffers) {
  auto CheckArray = [this](const std::shared_ptr<Array>& array) {
    auto f0 = field("f0", array->type());
    auto schema = ::arrow::schema({f0});
    auto batch = RecordBatch::Make(schema, array->length(), {array});
    auto sliced_batch = batch->Slice(0, 5);

    int64_t full_size;
    int64_t sliced_size;

    ASSERT_OK(GetRecordBatchSize(*batch, &full_size));
    ASSERT_OK(GetRecordBatchSize(*sliced_batch, &sliced_size));
    ASSERT_TRUE(sliced_size < full_size) << sliced_size << " " << full_size;

    // make sure we can write and read it
    this->CheckRoundtrip(*sliced_batch, 1 << 20);
  };

  std::shared_ptr<Array> a0, a1;
  auto pool = default_memory_pool();

  // Integer
  ASSERT_OK(MakeRandomInt32Array(500, false, pool, &a0));
  CheckArray(a0);

  // String / Binary
  {
    auto s = MakeRandomStringArray(500, false, pool, &a0);
    ASSERT_TRUE(s.ok());
  }
  CheckArray(a0);

  // Boolean
  ASSERT_OK(MakeRandomBooleanArray(10000, false, &a0));
  CheckArray(a0);

  // List
  ASSERT_OK(MakeRandomInt32Array(500, false, pool, &a0));
  ASSERT_OK(MakeRandomListArray(a0, 200, false, pool, &a1));
  CheckArray(a1);

  // Struct
  auto struct_type = struct_({field("f0", a0->type())});
  std::vector<std::shared_ptr<Array>> struct_children = {a0};
  a1 = std::make_shared<StructArray>(struct_type, a0->length(), struct_children);
  CheckArray(a1);

  // Sparse Union
  auto union_type = union_({field("f0", a0->type())}, {0});
  std::vector<int32_t> type_ids(a0->length());
  std::shared_ptr<Buffer> ids_buffer;
  ASSERT_OK(CopyBufferFromVector(type_ids, default_memory_pool(), &ids_buffer));
  a1 =
      std::make_shared<UnionArray>(union_type, a0->length(), struct_children, ids_buffer);
  CheckArray(a1);

  // Dense union
  auto dense_union_type = union_({field("f0", a0->type())}, {0}, UnionMode::DENSE);
  std::vector<int32_t> type_offsets;
  for (int32_t i = 0; i < a0->length(); ++i) {
    type_offsets.push_back(i);
  }
  std::shared_ptr<Buffer> offsets_buffer;
  ASSERT_OK(CopyBufferFromVector(type_offsets, default_memory_pool(), &offsets_buffer));
  a1 = std::make_shared<UnionArray>(dense_union_type, a0->length(), struct_children,
                                    ids_buffer, offsets_buffer);
  CheckArray(a1);
}

void TestGetRecordBatchSize(const IpcOptions& options,
                            std::shared_ptr<RecordBatch> batch) {
  io::MockOutputStream mock;
  int32_t mock_metadata_length = -1;
  int64_t mock_body_length = -1;
  int64_t size = -1;
  ASSERT_OK(WriteRecordBatch(*batch, 0, &mock, &mock_metadata_length, &mock_body_length,
                             options, default_memory_pool()));
  ASSERT_OK(GetRecordBatchSize(*batch, &size));
  ASSERT_EQ(mock.GetExtentBytesWritten(), size);
}

TEST_F(TestWriteRecordBatch, IntegerGetRecordBatchSize) {
  std::shared_ptr<RecordBatch> batch;

  ASSERT_OK(MakeIntRecordBatch(&batch));
  TestGetRecordBatchSize(options_, batch);

  ASSERT_OK(MakeListRecordBatch(&batch));
  TestGetRecordBatchSize(options_, batch);

  ASSERT_OK(MakeZeroLengthRecordBatch(&batch));
  TestGetRecordBatchSize(options_, batch);

  ASSERT_OK(MakeNonNullRecordBatch(&batch));
  TestGetRecordBatchSize(options_, batch);

  ASSERT_OK(MakeDeeplyNestedList(&batch));
  TestGetRecordBatchSize(options_, batch);
}

class RecursionLimits : public ::testing::Test, public io::MemoryMapFixture {
 public:
  void SetUp() { pool_ = default_memory_pool(); }
  void TearDown() { io::MemoryMapFixture::TearDown(); }

  Status WriteToMmap(int recursion_level, bool override_level, int32_t* metadata_length,
                     int64_t* body_length, std::shared_ptr<RecordBatch>* batch,
                     std::shared_ptr<Schema>* schema) {
    const int batch_length = 5;
    auto type = int32();
    std::shared_ptr<Array> array;
    const bool include_nulls = true;
    RETURN_NOT_OK(MakeRandomInt32Array(1000, include_nulls, pool_, &array));
    for (int i = 0; i < recursion_level; ++i) {
      type = list(type);
      RETURN_NOT_OK(
          MakeRandomListArray(array, batch_length, include_nulls, pool_, &array));
    }

    auto f0 = field("f0", type);

    *schema = ::arrow::schema({f0});

    *batch = RecordBatch::Make(*schema, batch_length, {array});

    std::stringstream ss;
    ss << "test-write-past-max-recursion-" << g_file_number++;
    const int memory_map_size = 1 << 20;
    ARROW_ASSIGN_OR_RAISE(mmap_,
                          io::MemoryMapFixture::InitMemoryMap(memory_map_size, ss.str()));

    auto options = IpcOptions::Defaults();
    if (override_level) {
      options.max_recursion_depth = recursion_level + 1;
    }
    return WriteRecordBatch(**batch, 0, mmap_.get(), metadata_length, body_length,
                            options, pool_);
  }

 protected:
  std::shared_ptr<io::MemoryMappedFile> mmap_;
  MemoryPool* pool_;
};

TEST_F(RecursionLimits, WriteLimit) {
  int32_t metadata_length = -1;
  int64_t body_length = -1;
  std::shared_ptr<Schema> schema;
  std::shared_ptr<RecordBatch> batch;
  ASSERT_RAISES(Invalid, WriteToMmap((1 << 8) + 1, false, &metadata_length, &body_length,
                                     &batch, &schema));
}

TEST_F(RecursionLimits, ReadLimit) {
  int32_t metadata_length = -1;
  int64_t body_length = -1;
  std::shared_ptr<Schema> schema;

  const int recursion_depth = 64;

  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK(WriteToMmap(recursion_depth, true, &metadata_length, &body_length, &batch,
                        &schema));

  std::unique_ptr<Message> message;
  ASSERT_OK(ReadMessage(0, metadata_length, mmap_.get(), &message));

  io::BufferReader reader(message->body());

  DictionaryMemo empty_memo;
  std::shared_ptr<RecordBatch> result;
  ASSERT_RAISES(Invalid, ReadRecordBatch(*message->metadata(), schema, &empty_memo,
                                         &reader, &result));
}

// Test fails with a structured exception on Windows + Debug
#if !defined(_WIN32) || defined(NDEBUG)
TEST_F(RecursionLimits, StressLimit) {
  auto CheckDepth = [this](int recursion_depth, bool* it_works) {
    int32_t metadata_length = -1;
    int64_t body_length = -1;
    std::shared_ptr<Schema> schema;
    std::shared_ptr<RecordBatch> batch;
    ASSERT_OK(WriteToMmap(recursion_depth, true, &metadata_length, &body_length, &batch,
                          &schema));

    std::unique_ptr<Message> message;
    ASSERT_OK(ReadMessage(0, metadata_length, mmap_.get(), &message));

    DictionaryMemo empty_memo;

    auto options = IpcOptions::Defaults();
    options.max_recursion_depth = recursion_depth + 1;
    io::BufferReader reader(message->body());
    std::shared_ptr<RecordBatch> result;
    ASSERT_OK(ReadRecordBatch(*message->metadata(), schema, &empty_memo, options, &reader,
                              &result));
    *it_works = result->Equals(*batch);
  };

  bool it_works = false;
  CheckDepth(100, &it_works);
  ASSERT_TRUE(it_works);

// Mitigate Valgrind's slowness
#if !defined(ARROW_VALGRIND)
  CheckDepth(500, &it_works);
  ASSERT_TRUE(it_works);
#endif
}
#endif  // !defined(_WIN32) || defined(NDEBUG)

struct FileWriterHelper {
  Status Init(const std::shared_ptr<Schema>& schema, const IpcOptions& options) {
    num_batches_written_ = 0;

    RETURN_NOT_OK(AllocateResizableBuffer(0, &buffer_));
    sink_.reset(new io::BufferOutputStream(buffer_));
    ARROW_ASSIGN_OR_RAISE(writer_,
                          RecordBatchFileWriter::Open(sink_.get(), schema, options));
    return Status::OK();
  }

  Status WriteBatch(const std::shared_ptr<RecordBatch>& batch) {
    RETURN_NOT_OK(writer_->WriteRecordBatch(*batch));
    num_batches_written_++;
    return Status::OK();
  }

  Status Finish() {
    RETURN_NOT_OK(writer_->Close());
    RETURN_NOT_OK(sink_->Close());
    // Current offset into stream is the end of the file
    return sink_->Tell().Value(&footer_offset_);
  }

  Status ReadBatches(BatchVector* out_batches) {
    auto buf_reader = std::make_shared<io::BufferReader>(buffer_);
    std::shared_ptr<RecordBatchFileReader> reader;
    RETURN_NOT_OK(RecordBatchFileReader::Open(buf_reader.get(), footer_offset_, &reader));

    EXPECT_EQ(num_batches_written_, reader->num_record_batches());
    for (int i = 0; i < num_batches_written_; ++i) {
      std::shared_ptr<RecordBatch> chunk;
      RETURN_NOT_OK(reader->ReadRecordBatch(i, &chunk));
      out_batches->push_back(chunk);
    }

    return Status::OK();
  }

  std::shared_ptr<ResizableBuffer> buffer_;
  std::unique_ptr<io::BufferOutputStream> sink_;
  std::shared_ptr<RecordBatchWriter> writer_;
  int num_batches_written_;
  int64_t footer_offset_;
};

struct StreamWriterHelper {
  Status Init(const std::shared_ptr<Schema>& schema, const IpcOptions& options) {
    RETURN_NOT_OK(AllocateResizableBuffer(0, &buffer_));
    sink_.reset(new io::BufferOutputStream(buffer_));
    ARROW_ASSIGN_OR_RAISE(writer_,
                          RecordBatchStreamWriter::Open(sink_.get(), schema, options));
    return Status::OK();
  }

  Status WriteBatch(const std::shared_ptr<RecordBatch>& batch) {
    RETURN_NOT_OK(writer_->WriteRecordBatch(*batch));
    return Status::OK();
  }

  Status Finish() {
    RETURN_NOT_OK(writer_->Close());
    return sink_->Close();
  }

  Status ReadBatches(BatchVector* out_batches) {
    auto buf_reader = std::make_shared<io::BufferReader>(buffer_);
    std::shared_ptr<RecordBatchReader> reader;
    RETURN_NOT_OK(RecordBatchStreamReader::Open(buf_reader, &reader));
    return reader->ReadAll(out_batches);
  }

  std::shared_ptr<ResizableBuffer> buffer_;
  std::unique_ptr<io::BufferOutputStream> sink_;
  std::shared_ptr<RecordBatchWriter> writer_;
};

// Parameterized mixin with tests for RecordBatchStreamWriter / RecordBatchFileWriter

template <class WriterHelperType>
class ReaderWriterMixin {
 public:
  using WriterHelper = WriterHelperType;

  // Check simple RecordBatch roundtripping
  template <typename Param>
  void TestRoundTrip(Param&& param, const IpcOptions& options) {
    std::shared_ptr<RecordBatch> batch1;
    std::shared_ptr<RecordBatch> batch2;
    ASSERT_OK(param(&batch1));  // NOLINT clang-tidy gtest issue
    ASSERT_OK(param(&batch2));  // NOLINT clang-tidy gtest issue

    BatchVector in_batches = {batch1, batch2};
    BatchVector out_batches;

    ASSERT_OK(RoundTripHelper(in_batches, options, &out_batches));
    ASSERT_EQ(out_batches.size(), in_batches.size());

    // Compare batches
    for (size_t i = 0; i < in_batches.size(); ++i) {
      CompareBatch(*in_batches[i], *out_batches[i]);
    }
  }

  template <typename Param>
  void TestZeroLengthRoundTrip(Param&& param, const IpcOptions& options) {
    std::shared_ptr<RecordBatch> batch1;
    std::shared_ptr<RecordBatch> batch2;
    ASSERT_OK(param(&batch1));  // NOLINT clang-tidy gtest issue
    ASSERT_OK(param(&batch2));  // NOLINT clang-tidy gtest issue
    batch1 = batch1->Slice(0, 0);
    batch2 = batch2->Slice(0, 0);

    BatchVector in_batches = {batch1, batch2};
    BatchVector out_batches;

    ASSERT_OK(RoundTripHelper(in_batches, options, &out_batches));
    ASSERT_EQ(out_batches.size(), in_batches.size());

    // Compare batches
    for (size_t i = 0; i < in_batches.size(); ++i) {
      CompareBatch(*in_batches[i], *out_batches[i]);
    }
  }

  void TestDictionaryRoundtrip() {
    std::shared_ptr<RecordBatch> batch;
    ASSERT_OK(MakeDictionary(&batch));

    BatchVector out_batches;
    ASSERT_OK(RoundTripHelper({batch}, IpcOptions::Defaults(), &out_batches));
    ASSERT_EQ(out_batches.size(), 1);

    // TODO(wesm): This was broken in ARROW-3144. I'm not sure how to
    // restore the deduplication logic yet because dictionaries are
    // corresponded to the Schema using Field pointers rather than
    // DataType as before

    // CheckDictionariesDeduplicated(*out_batches[0]);
  }

  void TestWriteDifferentSchema() {
    // Test writing batches with a different schema than the RecordBatchWriter
    // was initialized with.
    std::shared_ptr<RecordBatch> batch_ints, batch_bools;
    ASSERT_OK(MakeIntRecordBatch(&batch_ints));
    ASSERT_OK(MakeBooleanBatch(&batch_bools));

    std::shared_ptr<Schema> schema = batch_bools->schema();
    ASSERT_FALSE(schema->HasMetadata());
    schema = schema->WithMetadata(key_value_metadata({"some_key"}, {"some_value"}));

    WriterHelper writer_helper;
    ASSERT_OK(writer_helper.Init(schema, IpcOptions::Defaults()));
    // Writing a record batch with a different schema
    ASSERT_RAISES(Invalid, writer_helper.WriteBatch(batch_ints));
    // Writing a record batch with the same schema (except metadata)
    ASSERT_OK(writer_helper.WriteBatch(batch_bools));
    ASSERT_OK(writer_helper.Finish());

    // The single successful batch can be read again
    BatchVector out_batches;
    ASSERT_OK(writer_helper.ReadBatches(&out_batches));
    ASSERT_EQ(out_batches.size(), 1);
    CompareBatch(*out_batches[0], *batch_bools, false /* compare_metadata */);
    // Metadata from the RecordBatchWriter initialization schema was kept
    ASSERT_TRUE(out_batches[0]->schema()->Equals(*schema));
  }

 private:
  Status RoundTripHelper(const BatchVector& in_batches, const IpcOptions& options,
                         BatchVector* out_batches) {
    WriterHelper writer_helper;
    RETURN_NOT_OK(writer_helper.Init(in_batches[0]->schema(), options));
    for (const auto& batch : in_batches) {
      RETURN_NOT_OK(writer_helper.WriteBatch(batch));
    }
    RETURN_NOT_OK(writer_helper.Finish());
    RETURN_NOT_OK(writer_helper.ReadBatches(out_batches));
    for (const auto& batch : *out_batches) {
      RETURN_NOT_OK(batch->ValidateFull());
    }
    return Status::OK();
  }

  void CheckBatchDictionaries(const RecordBatch& batch) {
    // Check that dictionaries that should be the same are the same
    auto schema = batch.schema();

    const auto& b0 = checked_cast<const DictionaryArray&>(*batch.column(0));
    const auto& b1 = checked_cast<const DictionaryArray&>(*batch.column(1));

    ASSERT_EQ(b0.dictionary().get(), b1.dictionary().get());

    // Same dictionary used for list values
    const auto& b3 = checked_cast<const ListArray&>(*batch.column(3));
    const auto& b3_value = checked_cast<const DictionaryArray&>(*b3.values());
    ASSERT_EQ(b0.dictionary().get(), b3_value.dictionary().get());
  }
};

class TestFileFormat : public ReaderWriterMixin<FileWriterHelper>,
                       public ::testing::TestWithParam<MakeRecordBatch*> {};

class TestStreamFormat : public ReaderWriterMixin<StreamWriterHelper>,
                         public ::testing::TestWithParam<MakeRecordBatch*> {};

TEST_P(TestFileFormat, RoundTrip) {
  TestRoundTrip(*GetParam(), IpcOptions::Defaults());
  TestZeroLengthRoundTrip(*GetParam(), IpcOptions::Defaults());

  IpcOptions options;
  options.write_legacy_ipc_format = true;
  TestRoundTrip(*GetParam(), options);
  TestZeroLengthRoundTrip(*GetParam(), options);
}

TEST_P(TestStreamFormat, RoundTrip) {
  TestRoundTrip(*GetParam(), IpcOptions::Defaults());
  TestZeroLengthRoundTrip(*GetParam(), IpcOptions::Defaults());

  IpcOptions options;
  options.write_legacy_ipc_format = true;
  TestRoundTrip(*GetParam(), options);
  TestZeroLengthRoundTrip(*GetParam(), options);
}

INSTANTIATE_TEST_CASE_P(GenericIpcRoundTripTests, TestIpcRoundTrip, BATCH_CASES());
INSTANTIATE_TEST_CASE_P(FileRoundTripTests, TestFileFormat, BATCH_CASES());
INSTANTIATE_TEST_CASE_P(StreamRoundTripTests, TestStreamFormat, BATCH_CASES());

// This test uses uninitialized memory

#if !(defined(ARROW_VALGRIND) || defined(ADDRESS_SANITIZER))
TEST_F(TestIpcRoundTrip, LargeRecordBatch) {
  const int64_t length = static_cast<int64_t>(std::numeric_limits<int32_t>::max()) + 1;

  BooleanBuilder builder(default_memory_pool());
  ASSERT_OK(builder.Reserve(length));
  ASSERT_OK(builder.Advance(length));

  std::shared_ptr<Array> array;
  ASSERT_OK(builder.Finish(&array));

  auto f0 = arrow::field("f0", array->type());
  std::vector<std::shared_ptr<Field>> fields = {f0};
  auto schema = std::make_shared<Schema>(fields);

  auto batch = RecordBatch::Make(schema, length, {array});

  std::string path = "test-write-large-record_batch";

  // 512 MB
  constexpr int64_t kBufferSize = 1 << 29;
  ASSERT_OK_AND_ASSIGN(mmap_, io::MemoryMapFixture::InitMemoryMap(kBufferSize, path));

  std::shared_ptr<RecordBatch> result;
  ASSERT_OK(DoLargeRoundTrip(*batch, false, &result));
  CheckReadResult(*result, *batch);

  ASSERT_EQ(length, result->num_rows());
}
#endif

TEST_F(TestStreamFormat, DictionaryRoundTrip) { TestDictionaryRoundtrip(); }

TEST_F(TestFileFormat, DictionaryRoundTrip) { TestDictionaryRoundtrip(); }

TEST_F(TestStreamFormat, DifferentSchema) { TestWriteDifferentSchema(); }

TEST_F(TestFileFormat, DifferentSchema) { TestWriteDifferentSchema(); }

TEST(TestRecordBatchStreamReader, EmptyStreamWithDictionaries) {
  // ARROW-6006
  auto f0 = arrow::field("f0", arrow::dictionary(arrow::int8(), arrow::utf8()));
  auto schema = arrow::schema({f0});

  ASSERT_OK_AND_ASSIGN(auto stream, io::BufferOutputStream::Create(0));

  std::shared_ptr<RecordBatchWriter> writer;
  ASSERT_OK(RecordBatchStreamWriter::Open(stream.get(), schema, &writer));
  ASSERT_OK(writer->Close());

  ASSERT_OK_AND_ASSIGN(auto buffer, stream->Finish());
  io::BufferReader buffer_reader(buffer);
  std::shared_ptr<RecordBatchReader> reader;
  ASSERT_OK(RecordBatchStreamReader::Open(&buffer_reader, &reader));

  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK(reader->ReadNext(&batch));
  ASSERT_EQ(nullptr, batch);
}

// Delimit IPC stream messages and reassemble with the indicated messages
// included. This way we can remove messages from an IPC stream to test
// different failure modes or other difficult-to-test behaviors
void SpliceMessages(std::shared_ptr<Buffer> stream,
                    const std::vector<int>& included_indices,
                    std::shared_ptr<Buffer>* spliced_stream) {
  ASSERT_OK_AND_ASSIGN(auto out, io::BufferOutputStream::Create(0));

  io::BufferReader buffer_reader(stream);
  std::unique_ptr<MessageReader> message_reader = MessageReader::Open(&buffer_reader);
  std::unique_ptr<Message> msg;

  // Parse and reassemble first two messages in stream
  int message_index = 0;
  while (true) {
    ASSERT_OK(message_reader->ReadNextMessage(&msg));
    if (!msg) {
      break;
    }

    if (std::find(included_indices.begin(), included_indices.end(), message_index++) ==
        included_indices.end()) {
      // Message being dropped, continue
      continue;
    }

    IpcOptions options;
    internal::IpcPayload payload;
    payload.type = msg->type();
    payload.metadata = msg->metadata();
    payload.body_buffers.push_back(msg->body());
    payload.body_length = msg->body()->size();
    int32_t unused_metadata_length = -1;
    ASSERT_OK(
        internal::WriteIpcPayload(payload, options, out.get(), &unused_metadata_length));
  }
  ASSERT_OK_AND_ASSIGN(*spliced_stream, out->Finish());
}

TEST(TestRecordBatchStreamReader, NotEnoughDictionaries) {
  // ARROW-6126
  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK(MakeDictionaryFlat(&batch));

  ASSERT_OK_AND_ASSIGN(auto out, io::BufferOutputStream::Create(0));
  std::shared_ptr<RecordBatchWriter> writer;
  ASSERT_OK(RecordBatchStreamWriter::Open(out.get(), batch->schema(), &writer));
  ASSERT_OK(writer->WriteRecordBatch(*batch));
  ASSERT_OK(writer->Close());

  // Now let's mangle the stream a little bit and make sure we return the right
  // error
  ASSERT_OK_AND_ASSIGN(auto buffer, out->Finish());

  auto AssertFailsWith = [](std::shared_ptr<Buffer> stream, const std::string& ex_error) {
    io::BufferReader reader(stream);
    std::shared_ptr<RecordBatchReader> ipc_reader;
    ASSERT_OK(RecordBatchStreamReader::Open(&reader, &ipc_reader));
    std::shared_ptr<RecordBatch> batch;
    Status s = ipc_reader->ReadNext(&batch);
    ASSERT_TRUE(s.IsInvalid());
    ASSERT_EQ(ex_error, s.message().substr(0, ex_error.size()));
  };

  // Stream terminates before reading all dictionaries
  std::shared_ptr<Buffer> truncated_stream;
  SpliceMessages(buffer, {0, 1}, &truncated_stream);
  std::string ex_message =
      ("IPC stream ended without reading the expected number (3)"
       " of dictionaries");
  AssertFailsWith(truncated_stream, ex_message);

  // One of the dictionaries is missing, then we see a record batch
  SpliceMessages(buffer, {0, 1, 2, 4}, &truncated_stream);
  ex_message =
      ("IPC stream did not have the expected number (3) of dictionaries "
       "at the start of the stream");
  AssertFailsWith(truncated_stream, ex_message);
}

class TestTensorRoundTrip : public ::testing::Test, public IpcTestFixture {
 public:
  void SetUp() { IpcTestFixture::SetUp(); }
  void TearDown() { IpcTestFixture::TearDown(); }

  void CheckTensorRoundTrip(const Tensor& tensor) {
    int32_t metadata_length;
    int64_t body_length;

    const auto& type = checked_cast<const FixedWidthType&>(*tensor.type());
    const int elem_size = type.bit_width() / 8;

    ASSERT_OK(mmap_->Seek(0));

    ASSERT_OK(WriteTensor(tensor, mmap_.get(), &metadata_length, &body_length));

    const int64_t expected_body_length = elem_size * tensor.size();
    ASSERT_EQ(expected_body_length, body_length);

    ASSERT_OK(mmap_->Seek(0));

    std::shared_ptr<Tensor> result;
    ASSERT_OK_AND_ASSIGN(result, ReadTensor(mmap_.get()));

    ASSERT_EQ(result->data()->size(), expected_body_length);
    ASSERT_TRUE(tensor.Equals(*result));
  }
};

TEST_F(TestTensorRoundTrip, BasicRoundtrip) {
  std::string path = "test-write-tensor";
  constexpr int64_t kBufferSize = 1 << 20;
  ASSERT_OK_AND_ASSIGN(mmap_, io::MemoryMapFixture::InitMemoryMap(kBufferSize, path));

  std::vector<int64_t> shape = {4, 6};
  std::vector<int64_t> strides = {48, 8};
  std::vector<std::string> dim_names = {"foo", "bar"};
  int64_t size = 24;

  std::vector<int64_t> values;
  randint(size, 0, 100, &values);

  auto data = Buffer::Wrap(values);

  Tensor t0(int64(), data, shape, strides, dim_names);
  Tensor t_no_dims(int64(), data, {}, {}, {});
  Tensor t_zero_length_dim(int64(), data, {0}, {8}, {"foo"});

  CheckTensorRoundTrip(t0);
  CheckTensorRoundTrip(t_no_dims);
  CheckTensorRoundTrip(t_zero_length_dim);

  int64_t serialized_size;
  ASSERT_OK(GetTensorSize(t0, &serialized_size));
  ASSERT_TRUE(serialized_size > static_cast<int64_t>(size * sizeof(int64_t)));

  // ARROW-2840: Check that padding/alignment minded
  std::vector<int64_t> shape_2 = {1, 1};
  std::vector<int64_t> strides_2 = {8, 8};
  Tensor t0_not_multiple_64(int64(), data, shape_2, strides_2, dim_names);
  CheckTensorRoundTrip(t0_not_multiple_64);
}

TEST_F(TestTensorRoundTrip, NonContiguous) {
  std::string path = "test-write-tensor-strided";
  constexpr int64_t kBufferSize = 1 << 20;
  ASSERT_OK_AND_ASSIGN(mmap_, io::MemoryMapFixture::InitMemoryMap(kBufferSize, path));

  std::vector<int64_t> values;
  randint(24, 0, 100, &values);

  auto data = Buffer::Wrap(values);
  Tensor tensor(int64(), data, {4, 3}, {48, 16});

  CheckTensorRoundTrip(tensor);
}

template <typename IndexValueType>
class TestSparseTensorRoundTrip : public ::testing::Test, public IpcTestFixture {
 public:
  void SetUp() { IpcTestFixture::SetUp(); }
  void TearDown() { IpcTestFixture::TearDown(); }

  void CheckSparseCOOTensorRoundTrip(const SparseCOOTensor& sparse_tensor) {
    const auto& type = checked_cast<const FixedWidthType&>(*sparse_tensor.type());
    const int elem_size = type.bit_width() / 8;
    const int index_elem_size = sizeof(typename IndexValueType::c_type);

    int32_t metadata_length;
    int64_t body_length;

    ASSERT_OK(mmap_->Seek(0));

    ASSERT_OK(
        WriteSparseTensor(sparse_tensor, mmap_.get(), &metadata_length, &body_length));

    const auto& sparse_index =
        checked_cast<const SparseCOOIndex&>(*sparse_tensor.sparse_index());
    const int64_t indices_length =
        BitUtil::RoundUpToMultipleOf8(index_elem_size * sparse_index.indices()->size());
    const int64_t data_length =
        BitUtil::RoundUpToMultipleOf8(elem_size * sparse_tensor.non_zero_length());
    const int64_t expected_body_length = indices_length + data_length;
    ASSERT_EQ(expected_body_length, body_length);

    ASSERT_OK(mmap_->Seek(0));

    std::shared_ptr<SparseTensor> result;
    ASSERT_OK_AND_ASSIGN(result, ReadSparseTensor(mmap_.get()));
    ASSERT_EQ(SparseTensorFormat::COO, result->format_id());

    const auto& resulted_sparse_index =
        checked_cast<const SparseCOOIndex&>(*result->sparse_index());
    ASSERT_EQ(resulted_sparse_index.indices()->data()->size(), indices_length);
    ASSERT_EQ(result->data()->size(), data_length);
    ASSERT_TRUE(result->Equals(sparse_tensor));
  }

  template <typename SparseIndexType>
  void CheckSparseCSXMatrixRoundTrip(
      const SparseTensorImpl<SparseIndexType>& sparse_tensor) {
    static_assert(std::is_same<SparseIndexType, SparseCSRIndex>::value ||
                      std::is_same<SparseIndexType, SparseCSCIndex>::value,
                  "SparseIndexType must be either SparseCSRIndex or SparseCSCIndex");

    const auto& type = checked_cast<const FixedWidthType&>(*sparse_tensor.type());
    const int elem_size = type.bit_width() / 8;
    const int index_elem_size = sizeof(typename IndexValueType::c_type);

    int32_t metadata_length;
    int64_t body_length;

    ASSERT_OK(mmap_->Seek(0));

    ASSERT_OK(
        WriteSparseTensor(sparse_tensor, mmap_.get(), &metadata_length, &body_length));

    const auto& sparse_index =
        checked_cast<const SparseIndexType&>(*sparse_tensor.sparse_index());
    const int64_t indptr_length =
        BitUtil::RoundUpToMultipleOf8(index_elem_size * sparse_index.indptr()->size());
    const int64_t indices_length =
        BitUtil::RoundUpToMultipleOf8(index_elem_size * sparse_index.indices()->size());
    const int64_t data_length =
        BitUtil::RoundUpToMultipleOf8(elem_size * sparse_tensor.non_zero_length());
    const int64_t expected_body_length = indptr_length + indices_length + data_length;
    ASSERT_EQ(expected_body_length, body_length);

    ASSERT_OK(mmap_->Seek(0));

    std::shared_ptr<SparseTensor> result;
    ASSERT_OK_AND_ASSIGN(result, ReadSparseTensor(mmap_.get()));

    constexpr auto expected_format_id =
        std::is_same<SparseIndexType, SparseCSRIndex>::value ? SparseTensorFormat::CSR
                                                             : SparseTensorFormat::CSC;
    ASSERT_EQ(expected_format_id, result->format_id());

    const auto& resulted_sparse_index =
        checked_cast<const SparseIndexType&>(*result->sparse_index());
    ASSERT_EQ(resulted_sparse_index.indptr()->data()->size(), indptr_length);
    ASSERT_EQ(resulted_sparse_index.indices()->data()->size(), indices_length);
    ASSERT_EQ(result->data()->size(), data_length);
    ASSERT_TRUE(result->Equals(sparse_tensor));
  }

 protected:
  std::shared_ptr<SparseCOOIndex> MakeSparseCOOIndex(
      const std::vector<int64_t>& coords_shape,
      const std::vector<int64_t>& coords_strides,
      std::vector<typename IndexValueType::c_type>& coords_values) const {
    auto coords_data = Buffer::Wrap(coords_values);
    auto coords = std::make_shared<NumericTensor<IndexValueType>>(
        coords_data, coords_shape, coords_strides);
    return std::make_shared<SparseCOOIndex>(coords);
  }

  template <typename ValueType>
  std::shared_ptr<SparseCOOTensor> MakeSparseCOOTensor(
      const std::shared_ptr<SparseCOOIndex>& si, std::vector<ValueType>& sparse_values,
      const std::vector<int64_t>& shape,
      const std::vector<std::string>& dim_names = {}) const {
    auto data = Buffer::Wrap(sparse_values);
    return std::make_shared<SparseCOOTensor>(si, CTypeTraits<ValueType>::type_singleton(),
                                             data, shape, dim_names);
  }
};

TYPED_TEST_CASE_P(TestSparseTensorRoundTrip);

TYPED_TEST_P(TestSparseTensorRoundTrip, WithSparseCOOIndexRowMajor) {
  using IndexValueType = TypeParam;
  using c_index_value_type = typename IndexValueType::c_type;

  std::string path = "test-write-sparse-coo-tensor";
  constexpr int64_t kBufferSize = 1 << 20;
  ASSERT_OK_AND_ASSIGN(this->mmap_,
                       io::MemoryMapFixture::InitMemoryMap(kBufferSize, path));

  // Dense representation:
  // [
  //   [
  //     1 0 2 0
  //     0 3 0 4
  //     5 0 6 0
  //   ],
  //   [
  //      0 11  0 12
  //     13  0 14  0
  //      0 15  0 16
  //   ]
  // ]
  //
  // Sparse representation:
  // idx[0] = [0 0 0 0 0 0  1  1  1  1  1  1]
  // idx[1] = [0 0 1 1 2 2  0  0  1  1  2  2]
  // idx[2] = [0 2 1 3 0 2  1  3  0  2  1  3]
  // data   = [1 2 3 4 5 6 11 12 13 14 15 16]

  std::vector<c_index_value_type> coords_values = {0, 0, 0, 0, 0, 2, 0, 1, 1, 0, 1, 3,
                                                   0, 2, 0, 0, 2, 2, 1, 0, 1, 1, 0, 3,
                                                   1, 1, 0, 1, 1, 2, 1, 2, 1, 1, 2, 3};
  const int sizeof_index_value = sizeof(c_index_value_type);
  std::shared_ptr<SparseCOOIndex> si;
  ASSERT_OK_AND_ASSIGN(
      si, SparseCOOIndex::Make(TypeTraits<IndexValueType>::type_singleton(), {12, 3},
                               {sizeof_index_value * 3, sizeof_index_value},
                               Buffer::Wrap(coords_values)));

  std::vector<int64_t> shape = {2, 3, 4};
  std::vector<std::string> dim_names = {"foo", "bar", "baz"};
  std::vector<int64_t> values = {1, 2, 3, 4, 5, 6, 11, 12, 13, 14, 15, 16};
  auto st = this->MakeSparseCOOTensor(si, values, shape, dim_names);

  this->CheckSparseCOOTensorRoundTrip(*st);
}

TYPED_TEST_P(TestSparseTensorRoundTrip, WithSparseCOOIndexColumnMajor) {
  using IndexValueType = TypeParam;
  using c_index_value_type = typename IndexValueType::c_type;

  std::string path = "test-write-sparse-coo-tensor";
  constexpr int64_t kBufferSize = 1 << 20;
  ASSERT_OK_AND_ASSIGN(this->mmap_,
                       io::MemoryMapFixture::InitMemoryMap(kBufferSize, path));

  // Dense representation:
  // [
  //   [
  //     1 0 2 0
  //     0 3 0 4
  //     5 0 6 0
  //   ],
  //   [
  //      0 11  0 12
  //     13  0 14  0
  //      0 15  0 16
  //   ]
  // ]
  //
  // Sparse representation:
  // idx[0] = [0 0 0 0 0 0  1  1  1  1  1  1]
  // idx[1] = [0 0 1 1 2 2  0  0  1  1  2  2]
  // idx[2] = [0 2 1 3 0 2  1  3  0  2  1  3]
  // data   = [1 2 3 4 5 6 11 12 13 14 15 16]

  std::vector<c_index_value_type> coords_values = {0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1,
                                                   0, 0, 1, 1, 2, 2, 0, 0, 1, 1, 2, 2,
                                                   0, 2, 1, 3, 0, 2, 1, 3, 0, 2, 1, 3};
  const int sizeof_index_value = sizeof(c_index_value_type);
  std::shared_ptr<SparseCOOIndex> si;
  ASSERT_OK_AND_ASSIGN(
      si, SparseCOOIndex::Make(TypeTraits<IndexValueType>::type_singleton(), {12, 3},
                               {sizeof_index_value, sizeof_index_value * 12},
                               Buffer::Wrap(coords_values)));

  std::vector<int64_t> shape = {2, 3, 4};
  std::vector<std::string> dim_names = {"foo", "bar", "baz"};
  std::vector<int64_t> values = {1, 2, 3, 4, 5, 6, 11, 12, 13, 14, 15, 16};
  auto st = this->MakeSparseCOOTensor(si, values, shape, dim_names);

  this->CheckSparseCOOTensorRoundTrip(*st);
}

TYPED_TEST_P(TestSparseTensorRoundTrip, WithSparseCSRIndex) {
  using IndexValueType = TypeParam;

  std::string path = "test-write-sparse-csr-matrix";
  constexpr int64_t kBufferSize = 1 << 20;
  ASSERT_OK_AND_ASSIGN(this->mmap_,
                       io::MemoryMapFixture::InitMemoryMap(kBufferSize, path));

  std::vector<int64_t> shape = {4, 6};
  std::vector<std::string> dim_names = {"foo", "bar", "baz"};
  std::vector<int64_t> values = {1, 0,  2, 0,  0,  3, 0,  4, 5, 0,  6, 0,
                                 0, 11, 0, 12, 13, 0, 14, 0, 0, 15, 0, 16};

  auto data = Buffer::Wrap(values);
  NumericTensor<Int64Type> t(data, shape, {}, dim_names);
  std::shared_ptr<SparseCSRMatrix> st;
  ASSERT_OK_AND_ASSIGN(
      st, SparseCSRMatrix::Make(t, TypeTraits<IndexValueType>::type_singleton()));

  this->CheckSparseCSXMatrixRoundTrip(*st);
}

TYPED_TEST_P(TestSparseTensorRoundTrip, WithSparseCSCIndex) {
  using IndexValueType = TypeParam;

  std::string path = "test-write-sparse-csc-matrix";
  constexpr int64_t kBufferSize = 1 << 20;
  ASSERT_OK_AND_ASSIGN(this->mmap_,
                       io::MemoryMapFixture::InitMemoryMap(kBufferSize, path));

  std::vector<int64_t> shape = {4, 6};
  std::vector<std::string> dim_names = {"foo", "bar", "baz"};
  std::vector<int64_t> values = {1, 0,  2, 0,  0,  3, 0,  4, 5, 0,  6, 0,
                                 0, 11, 0, 12, 13, 0, 14, 0, 0, 15, 0, 16};

  auto data = Buffer::Wrap(values);
  NumericTensor<Int64Type> t(data, shape, {}, dim_names);
  std::shared_ptr<SparseCSCMatrix> st;
  ASSERT_OK_AND_ASSIGN(
      st, SparseCSCMatrix::Make(t, TypeTraits<IndexValueType>::type_singleton()));

  this->CheckSparseCSXMatrixRoundTrip(*st);
}

REGISTER_TYPED_TEST_CASE_P(TestSparseTensorRoundTrip, WithSparseCOOIndexRowMajor,
                           WithSparseCOOIndexColumnMajor, WithSparseCSRIndex,
                           WithSparseCSCIndex);

INSTANTIATE_TYPED_TEST_CASE_P(TestInt8, TestSparseTensorRoundTrip, Int8Type);
INSTANTIATE_TYPED_TEST_CASE_P(TestUInt8, TestSparseTensorRoundTrip, UInt8Type);
INSTANTIATE_TYPED_TEST_CASE_P(TestInt16, TestSparseTensorRoundTrip, Int16Type);
INSTANTIATE_TYPED_TEST_CASE_P(TestUInt16, TestSparseTensorRoundTrip, UInt16Type);
INSTANTIATE_TYPED_TEST_CASE_P(TestInt32, TestSparseTensorRoundTrip, Int32Type);
INSTANTIATE_TYPED_TEST_CASE_P(TestUInt32, TestSparseTensorRoundTrip, UInt32Type);
INSTANTIATE_TYPED_TEST_CASE_P(TestInt64, TestSparseTensorRoundTrip, Int64Type);
INSTANTIATE_TYPED_TEST_CASE_P(TestUInt64, TestSparseTensorRoundTrip, UInt64Type);

TEST(TestRecordBatchStreamReader, MalformedInput) {
  const std::string empty_str = "";
  const std::string garbage_str = "12345678";

  auto empty = std::make_shared<Buffer>(empty_str);
  auto garbage = std::make_shared<Buffer>(garbage_str);

  std::shared_ptr<RecordBatchReader> batch_reader;

  io::BufferReader empty_reader(empty);
  ASSERT_RAISES(Invalid, RecordBatchStreamReader::Open(&empty_reader, &batch_reader));

  io::BufferReader garbage_reader(garbage);
  ASSERT_RAISES(Invalid, RecordBatchStreamReader::Open(&garbage_reader, &batch_reader));
}

// ----------------------------------------------------------------------
// DictionaryMemo miscellanea

TEST(TestDictionaryMemo, ReusedDictionaries) {
  DictionaryMemo memo;

  std::shared_ptr<Field> field1 = field("a", dictionary(int8(), utf8()));
  std::shared_ptr<Field> field2 = field("b", dictionary(int16(), utf8()));

  // Two fields referencing the same dictionary_id
  int64_t dictionary_id = 0;
  auto dict = ArrayFromJSON(utf8(), "[\"foo\", \"bar\", \"baz\"]");

  ASSERT_OK(memo.AddField(dictionary_id, field1));
  ASSERT_OK(memo.AddField(dictionary_id, field2));

  std::shared_ptr<DataType> value_type;
  ASSERT_OK(memo.GetDictionaryType(dictionary_id, &value_type));
  ASSERT_TRUE(value_type->Equals(*utf8()));

  ASSERT_FALSE(memo.HasDictionary(dictionary_id));
  ASSERT_OK(memo.AddDictionary(dictionary_id, dict));
  ASSERT_TRUE(memo.HasDictionary(dictionary_id));

  ASSERT_EQ(2, memo.num_fields());
  ASSERT_EQ(1, memo.num_dictionaries());

  ASSERT_TRUE(memo.HasDictionary(*field1));
  ASSERT_TRUE(memo.HasDictionary(*field2));

  int64_t returned_id = -1;
  ASSERT_OK(memo.GetId(*field1, &returned_id));
  ASSERT_EQ(0, returned_id);
  returned_id = -1;
  ASSERT_OK(memo.GetId(*field2, &returned_id));
  ASSERT_EQ(0, returned_id);
}

}  // namespace test
}  // namespace ipc
}  // namespace arrow
