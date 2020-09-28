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

#include "arrow/ipc/feather.h"

#include <cstdint>
#include <cstring>
#include <memory>
#include <sstream>  // IWYU pragma: keep
#include <string>
#include <utility>
#include <vector>

#include <flatbuffers/flatbuffers.h>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/io/interfaces.h"
#include "arrow/ipc/feather_internal.h"
#include "arrow/ipc/util.h"  // IWYU pragma: keep
#include "arrow/status.h"
#include "arrow/table.h"  // IWYU pragma: keep
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/visitor.h"

#include "generated/feather_generated.h"

namespace arrow {

using internal::checked_cast;

namespace ipc {
namespace feather {

static const uint8_t kPaddingBytes[kFeatherDefaultAlignment] = {0};

static inline int64_t PaddedLength(int64_t nbytes) {
  static const int64_t alignment = kFeatherDefaultAlignment;
  return ((nbytes + alignment - 1) / alignment) * alignment;
}

// XXX: Hack for Feather 0.3.0 for backwards compatibility with old files
// Size in-file of written byte buffer
static int64_t GetOutputLength(int64_t nbytes) {
  if (kFeatherVersion < 2) {
    // Feather files < 0.3.0
    return nbytes;
  } else {
    return PaddedLength(nbytes);
  }
}

static Status WritePadded(io::OutputStream* stream, const uint8_t* data, int64_t length,
                          int64_t* bytes_written) {
  RETURN_NOT_OK(stream->Write(data, length));

  int64_t remainder = PaddedLength(length) - length;
  if (remainder != 0) {
    RETURN_NOT_OK(stream->Write(kPaddingBytes, remainder));
  }
  *bytes_written = length + remainder;
  return Status::OK();
}

static Status WritePaddedWithOffset(io::OutputStream* stream, const uint8_t* data,
                                    int64_t bit_offset, const int64_t length,
                                    int64_t* bytes_written) {
  data = data + bit_offset / 8;
  uint8_t bit_shift = static_cast<uint8_t>(bit_offset % 8);
  if (bit_offset == 0) {
    RETURN_NOT_OK(stream->Write(data, length));
  } else {
    constexpr int64_t buffersize = 256;
    uint8_t buffer[buffersize];
    const uint8_t lshift = static_cast<uint8_t>(8 - bit_shift);
    const uint8_t* buffer_end = buffer + buffersize;
    uint8_t* buffer_it = buffer;

    for (const uint8_t* end = data + length; data != end;) {
      uint8_t r = static_cast<uint8_t>(*data++ >> bit_shift);
      uint8_t l = static_cast<uint8_t>(*data << lshift);
      uint8_t value = l | r;
      *buffer_it++ = value;
      if (buffer_it == buffer_end) {
        RETURN_NOT_OK(stream->Write(buffer, buffersize));
        buffer_it = buffer;
      }
    }
    if (buffer_it != buffer) {
      RETURN_NOT_OK(stream->Write(buffer, buffer_it - buffer));
    }
  }

  int64_t remainder = PaddedLength(length) - length;
  if (remainder != 0) {
    RETURN_NOT_OK(stream->Write(kPaddingBytes, remainder));
  }
  *bytes_written = length + remainder;
  return Status::OK();
}

/// For compatibility, we need to write any data sometimes just to keep producing
/// files that can be read with an older reader.
static Status WritePaddedBlank(io::OutputStream* stream, int64_t length,
                               int64_t* bytes_written) {
  const uint8_t null = 0;
  for (int64_t i = 0; i < length; i++) {
    RETURN_NOT_OK(stream->Write(&null, 1));
  }

  int64_t remainder = PaddedLength(length) - length;
  if (remainder != 0) {
    RETURN_NOT_OK(stream->Write(kPaddingBytes, remainder));
  }
  *bytes_written = length + remainder;
  return Status::OK();
}

// ----------------------------------------------------------------------
// TableBuilder

TableBuilder::TableBuilder(int64_t num_rows) : finished_(false), num_rows_(num_rows) {}

FBB& TableBuilder::fbb() { return fbb_; }

Status TableBuilder::Finish() {
  if (finished_) {
    return Status::Invalid("can only call this once");
  }

  FBString desc = 0;
  if (!description_.empty()) {
    desc = fbb_.CreateString(description_);
  }

  flatbuffers::Offset<flatbuffers::String> metadata = 0;

  auto root = fbs::CreateCTable(fbb_, desc, num_rows_, fbb_.CreateVector(columns_),
                                kFeatherVersion, metadata);
  fbb_.Finish(root);
  finished_ = true;

  return Status::OK();
}

std::shared_ptr<Buffer> TableBuilder::GetBuffer() const {
  return std::make_shared<Buffer>(fbb_.GetBufferPointer(),
                                  static_cast<int64_t>(fbb_.GetSize()));
}

void TableBuilder::SetDescription(const std::string& description) {
  description_ = description;
}

void TableBuilder::SetNumRows(int64_t num_rows) { num_rows_ = num_rows; }

void TableBuilder::add_column(const flatbuffers::Offset<fbs::Column>& col) {
  columns_.push_back(col);
}

ColumnBuilder::ColumnBuilder(TableBuilder* parent, const std::string& name)
    : parent_(parent) {
  fbb_ = &parent->fbb();
  name_ = name;
  type_ = ColumnType::PRIMITIVE;
  meta_time_.unit = TimeUnit::SECOND;
}

flatbuffers::Offset<void> ColumnBuilder::CreateColumnMetadata() {
  switch (type_) {
    case ColumnType::PRIMITIVE:
      // flatbuffer void
      return 0;
    case ColumnType::CATEGORY: {
      auto cat_meta = fbs::CreateCategoryMetadata(
          fbb(), GetPrimitiveArray(fbb(), meta_category_.levels), meta_category_.ordered);
      return cat_meta.Union();
    }
    case ColumnType::TIMESTAMP: {
      // flatbuffer void
      flatbuffers::Offset<flatbuffers::String> tz = 0;
      if (!meta_timestamp_.timezone.empty()) {
        tz = fbb().CreateString(meta_timestamp_.timezone);
      }

      auto ts_meta =
          fbs::CreateTimestampMetadata(fbb(), ToFlatbufferEnum(meta_timestamp_.unit), tz);
      return ts_meta.Union();
    }
    case ColumnType::DATE: {
      auto date_meta = fbs::CreateDateMetadata(fbb());
      return date_meta.Union();
    }
    case ColumnType::TIME: {
      auto time_meta = fbs::CreateTimeMetadata(fbb(), ToFlatbufferEnum(meta_time_.unit));
      return time_meta.Union();
    }
    default:
      // null
      return flatbuffers::Offset<void>();
  }
}

Status ColumnBuilder::Finish() {
  FBB& buf = fbb();

  // values
  auto values = GetPrimitiveArray(buf, values_);
  flatbuffers::Offset<void> metadata = CreateColumnMetadata();

  auto column = fbs::CreateColumn(buf, buf.CreateString(name_), values,
                                  ToFlatbufferEnum(type_),  // metadata_type
                                  metadata, buf.CreateString(user_metadata_));

  // bad coupling, but OK for now
  parent_->add_column(column);
  return Status::OK();
}

void ColumnBuilder::SetValues(const ArrayMetadata& values) { values_ = values; }

void ColumnBuilder::SetUserMetadata(const std::string& data) { user_metadata_ = data; }

void ColumnBuilder::SetCategory(const ArrayMetadata& levels, bool ordered) {
  type_ = ColumnType::CATEGORY;
  meta_category_.levels = levels;
  meta_category_.ordered = ordered;
}

void ColumnBuilder::SetTimestamp(TimeUnit::type unit) {
  type_ = ColumnType::TIMESTAMP;
  meta_timestamp_.unit = unit;
}

void ColumnBuilder::SetTimestamp(TimeUnit::type unit, const std::string& timezone) {
  SetTimestamp(unit);
  meta_timestamp_.timezone = timezone;
}

void ColumnBuilder::SetDate() { type_ = ColumnType::DATE; }

void ColumnBuilder::SetTime(TimeUnit::type unit) {
  type_ = ColumnType::TIME;
  meta_time_.unit = unit;
}

FBB& ColumnBuilder::fbb() { return *fbb_; }

std::unique_ptr<ColumnBuilder> TableBuilder::AddColumn(const std::string& name) {
  return std::unique_ptr<ColumnBuilder>(new ColumnBuilder(this, name));
}

// ----------------------------------------------------------------------
// reader.cc

class TableReader::TableReaderImpl {
 public:
  TableReaderImpl() {}

  Status Open(const std::shared_ptr<io::RandomAccessFile>& source) {
    source_ = source;

    int magic_size = static_cast<int>(strlen(kFeatherMagicBytes));
    int footer_size = magic_size + static_cast<int>(sizeof(uint32_t));

    // Pathological issue where the file is smaller than
    ARROW_ASSIGN_OR_RAISE(int64_t size, source->GetSize());
    if (size < magic_size + footer_size) {
      return Status::Invalid("File is too small to be a well-formed file");
    }

    ARROW_ASSIGN_OR_RAISE(auto buffer, source->ReadAt(0, magic_size));

    if (memcmp(buffer->data(), kFeatherMagicBytes, magic_size)) {
      return Status::Invalid("Not a feather file");
    }

    // Now get the footer and verify
    ARROW_ASSIGN_OR_RAISE(buffer, source->ReadAt(size - footer_size, footer_size));

    if (memcmp(buffer->data() + sizeof(uint32_t), kFeatherMagicBytes, magic_size)) {
      return Status::Invalid("Feather file footer incomplete");
    }

    uint32_t metadata_length = *reinterpret_cast<const uint32_t*>(buffer->data());
    if (size < magic_size + footer_size + metadata_length) {
      return Status::Invalid("File is smaller than indicated metadata size");
    }
    ARROW_ASSIGN_OR_RAISE(
        buffer, source->ReadAt(size - footer_size - metadata_length, metadata_length));

    metadata_.reset(new TableMetadata());
    return metadata_->Open(buffer);
  }

  Status GetDataType(const fbs::PrimitiveArray* values, fbs::TypeMetadata metadata_type,
                     const void* metadata, std::shared_ptr<DataType>* out,
                     std::shared_ptr<Array>* out_dictionary = nullptr) {
#define PRIMITIVE_CASE(CAP_TYPE, FACTORY_FUNC) \
  case fbs::Type::CAP_TYPE:                    \
    *out = FACTORY_FUNC();                     \
    break;

    switch (metadata_type) {
      case fbs::TypeMetadata::CategoryMetadata: {
        auto meta = static_cast<const fbs::CategoryMetadata*>(metadata);

        std::shared_ptr<DataType> index_type;
        RETURN_NOT_OK(GetDataType(values, fbs::TypeMetadata::NONE, nullptr, &index_type));

        RETURN_NOT_OK(
            LoadValues(meta->levels(), fbs::TypeMetadata::NONE, nullptr, out_dictionary));

        *out = dictionary(index_type, (*out_dictionary)->type(), meta->ordered());
        break;
      }
      case fbs::TypeMetadata::TimestampMetadata: {
        auto meta = static_cast<const fbs::TimestampMetadata*>(metadata);
        TimeUnit::type unit = FromFlatbufferEnum(meta->unit());
        std::string tz;
        // flatbuffer non-null
        if (meta->timezone() != 0) {
          tz = meta->timezone()->str();
        } else {
          tz = "";
        }
        *out = timestamp(unit, tz);
      } break;
      case fbs::TypeMetadata::DateMetadata:
        *out = date32();
        break;
      case fbs::TypeMetadata::TimeMetadata: {
        auto meta = static_cast<const fbs::TimeMetadata*>(metadata);
        *out = time32(FromFlatbufferEnum(meta->unit()));
      } break;
      default:
        switch (values->type()) {
          PRIMITIVE_CASE(BOOL, boolean);
          PRIMITIVE_CASE(INT8, int8);
          PRIMITIVE_CASE(INT16, int16);
          PRIMITIVE_CASE(INT32, int32);
          PRIMITIVE_CASE(INT64, int64);
          PRIMITIVE_CASE(UINT8, uint8);
          PRIMITIVE_CASE(UINT16, uint16);
          PRIMITIVE_CASE(UINT32, uint32);
          PRIMITIVE_CASE(UINT64, uint64);
          PRIMITIVE_CASE(FLOAT, float32);
          PRIMITIVE_CASE(DOUBLE, float64);
          PRIMITIVE_CASE(UTF8, utf8);
          PRIMITIVE_CASE(BINARY, binary);
          PRIMITIVE_CASE(LARGE_UTF8, large_utf8);
          PRIMITIVE_CASE(LARGE_BINARY, large_binary);
          default:
            return Status::Invalid("Unrecognized type");
        }
        break;
    }

#undef PRIMITIVE_CASE

    return Status::OK();
  }

  // Retrieve a primitive array from the data source
  //
  // @returns: a Buffer instance, the precise type will depend on the kind of
  // input data source (which may or may not have memory-map like semantics)
  Status LoadValues(const fbs::PrimitiveArray* meta, fbs::TypeMetadata metadata_type,
                    const void* metadata, std::shared_ptr<Array>* out) {
    std::shared_ptr<DataType> type;
    std::shared_ptr<Array> dictionary;
    RETURN_NOT_OK(GetDataType(meta, metadata_type, metadata, &type, &dictionary));

    std::vector<std::shared_ptr<Buffer>> buffers;

    // Buffer data from the source (may or may not perform a copy depending on
    // input source)
    ARROW_ASSIGN_OR_RAISE(auto buffer,
                          source_->ReadAt(meta->offset(), meta->total_bytes()));

    int64_t offset = 0;

    // If there are nulls, the null bitmask is first
    if (meta->null_count() > 0) {
      int64_t null_bitmap_size = GetOutputLength(BitUtil::BytesForBits(meta->length()));
      buffers.push_back(SliceBuffer(buffer, offset, null_bitmap_size));
      offset += null_bitmap_size;
    } else {
      buffers.push_back(nullptr);
    }

    if (is_binary_like(type->id())) {
      int64_t offsets_size = GetOutputLength((meta->length() + 1) * sizeof(int32_t));
      buffers.push_back(SliceBuffer(buffer, offset, offsets_size));
      offset += offsets_size;
    } else if (is_large_binary_like(type->id())) {
      int64_t offsets_size = GetOutputLength((meta->length() + 1) * sizeof(int64_t));
      buffers.push_back(SliceBuffer(buffer, offset, offsets_size));
      offset += offsets_size;
    }

    buffers.push_back(SliceBuffer(buffer, offset, buffer->size() - offset));

    auto arr_data =
        ArrayData::Make(type, meta->length(), std::move(buffers), meta->null_count());
    arr_data->dictionary = dictionary;
    *out = MakeArray(arr_data);
    return Status::OK();
  }

  bool HasDescription() const { return metadata_->HasDescription(); }

  std::string GetDescription() const { return metadata_->GetDescription(); }

  int version() const { return metadata_->version(); }
  int64_t num_rows() const { return metadata_->num_rows(); }
  int64_t num_columns() const { return metadata_->num_columns(); }

  std::string GetColumnName(int i) const {
    const fbs::Column* col_meta = metadata_->column(i);
    return col_meta->name()->str();
  }

  Status GetColumn(int i, std::shared_ptr<ChunkedArray>* out) {
    const fbs::Column* col_meta = metadata_->column(i);

    // auto user_meta = column->user_metadata();
    // if (user_meta->size() > 0) { user_metadata_ = user_meta->str(); }

    std::shared_ptr<Array> values;
    RETURN_NOT_OK(LoadValues(col_meta->values(), col_meta->metadata_type(),
                             col_meta->metadata(), &values));
    *out = std::make_shared<ChunkedArray>(values);
    return Status::OK();
  }

  Status Read(std::shared_ptr<Table>* out) {
    std::vector<std::shared_ptr<Field>> fields;
    std::vector<std::shared_ptr<ChunkedArray>> columns;
    for (int i = 0; i < num_columns(); ++i) {
      std::shared_ptr<ChunkedArray> column;
      RETURN_NOT_OK(GetColumn(i, &column));
      columns.push_back(column);
      fields.push_back(::arrow::field(GetColumnName(i), column->type()));
    }
    *out = Table::Make(schema(fields), columns);
    return Status::OK();
  }

  Status Read(const std::vector<int>& indices, std::shared_ptr<Table>* out) {
    std::vector<std::shared_ptr<Field>> fields;
    std::vector<std::shared_ptr<ChunkedArray>> columns;
    for (int i = 0; i < num_columns(); ++i) {
      bool found = false;
      for (auto j : indices) {
        if (i == j) {
          found = true;
          break;
        }
      }
      if (!found) {
        continue;
      }
      std::shared_ptr<ChunkedArray> column;
      RETURN_NOT_OK(GetColumn(i, &column));
      columns.push_back(column);
      fields.push_back(::arrow::field(GetColumnName(i), column->type()));
    }
    *out = Table::Make(schema(fields), columns);
    return Status::OK();
  }

  Status Read(const std::vector<std::string>& names, std::shared_ptr<Table>* out) {
    std::vector<std::shared_ptr<Field>> fields;
    std::vector<std::shared_ptr<ChunkedArray>> columns;
    for (int i = 0; i < num_columns(); ++i) {
      auto name = GetColumnName(i);
      bool found = false;
      for (auto& n : names) {
        if (name == n) {
          found = true;
          break;
        }
      }
      if (!found) {
        continue;
      }
      std::shared_ptr<ChunkedArray> column;
      RETURN_NOT_OK(GetColumn(i, &column));
      columns.push_back(column);
      fields.push_back(::arrow::field(name, column->type()));
    }
    *out = Table::Make(schema(fields), columns);
    return Status::OK();
  }

 private:
  std::shared_ptr<io::RandomAccessFile> source_;
  std::unique_ptr<TableMetadata> metadata_;

  std::shared_ptr<Schema> schema_;
};

// ----------------------------------------------------------------------
// TableReader public API

TableReader::TableReader() { impl_.reset(new TableReaderImpl()); }

TableReader::~TableReader() {}

Status TableReader::Open(const std::shared_ptr<io::RandomAccessFile>& source,
                         std::unique_ptr<TableReader>* out) {
  out->reset(new TableReader());
  return (*out)->impl_->Open(source);
}

bool TableReader::HasDescription() const { return impl_->HasDescription(); }

std::string TableReader::GetDescription() const { return impl_->GetDescription(); }

int TableReader::version() const { return impl_->version(); }

int64_t TableReader::num_rows() const { return impl_->num_rows(); }

int64_t TableReader::num_columns() const { return impl_->num_columns(); }

std::string TableReader::GetColumnName(int i) const { return impl_->GetColumnName(i); }

Status TableReader::GetColumn(int i, std::shared_ptr<ChunkedArray>* out) {
  return impl_->GetColumn(i, out);
}

Status TableReader::Read(std::shared_ptr<Table>* out) { return impl_->Read(out); }

Status TableReader::Read(const std::vector<int>& indices, std::shared_ptr<Table>* out) {
  return impl_->Read(indices, out);
}

Status TableReader::Read(const std::vector<std::string>& names,
                         std::shared_ptr<Table>* out) {
  return impl_->Read(names, out);
}

// ----------------------------------------------------------------------
// writer.cc

fbs::Type ToFlatbufferType(Type::type type) {
  switch (type) {
    case Type::BOOL:
      return fbs::Type::BOOL;
    case Type::INT8:
      return fbs::Type::INT8;
    case Type::INT16:
      return fbs::Type::INT16;
    case Type::INT32:
      return fbs::Type::INT32;
    case Type::INT64:
      return fbs::Type::INT64;
    case Type::UINT8:
      return fbs::Type::UINT8;
    case Type::UINT16:
      return fbs::Type::UINT16;
    case Type::UINT32:
      return fbs::Type::UINT32;
    case Type::UINT64:
      return fbs::Type::UINT64;
    case Type::FLOAT:
      return fbs::Type::FLOAT;
    case Type::DOUBLE:
      return fbs::Type::DOUBLE;
    case Type::STRING:
      return fbs::Type::UTF8;
    case Type::BINARY:
      return fbs::Type::BINARY;
    case Type::LARGE_STRING:
      return fbs::Type::LARGE_UTF8;
    case Type::LARGE_BINARY:
      return fbs::Type::LARGE_BINARY;
    case Type::DATE32:
      return fbs::Type::INT32;
    case Type::TIMESTAMP:
      return fbs::Type::INT64;
    case Type::TIME32:
      return fbs::Type::INT32;
    case Type::TIME64:
      return fbs::Type::INT64;
    default:
      DCHECK(false) << "Cannot reach this code";
  }
  // prevent compiler warning
  return fbs::Type::MIN;
}

static Status SanitizeUnsupportedTypes(const Array& values, std::shared_ptr<Array>* out) {
  if (values.type_id() == Type::NA) {
    // As long as R doesn't support NA, we write this as a StringColumn
    // to ensure stable roundtrips.
    *out = std::make_shared<StringArray>(values.length(), nullptr, nullptr,
                                         values.null_bitmap(), values.null_count());
    return Status::OK();
  } else {
    *out = MakeArray(values.data());
    return Status::OK();
  }
}

class TableWriter::TableWriterImpl : public ArrayVisitor {
 public:
  TableWriterImpl() : initialized_stream_(false), metadata_(0) {}

  Status Open(const std::shared_ptr<io::OutputStream>& stream) {
    stream_ = stream;
    return Status::OK();
  }

  void SetDescription(const std::string& desc) { metadata_.SetDescription(desc); }

  void SetNumRows(int64_t num_rows) { metadata_.SetNumRows(num_rows); }

  Status Finalize() {
    RETURN_NOT_OK(CheckStarted());
    RETURN_NOT_OK(metadata_.Finish());

    auto buffer = metadata_.GetBuffer();

    // Writer metadata
    int64_t bytes_written;
    RETURN_NOT_OK(
        WritePadded(stream_.get(), buffer->data(), buffer->size(), &bytes_written));
    uint32_t buffer_size = static_cast<uint32_t>(bytes_written);

    // Footer: metadata length, magic bytes
    RETURN_NOT_OK(stream_->Write(&buffer_size, sizeof(uint32_t)));
    return stream_->Write(kFeatherMagicBytes, strlen(kFeatherMagicBytes));
  }

  Status LoadArrayMetadata(const Array& values, ArrayMetadata* meta) {
    if (!(is_primitive(values.type_id()) || is_binary_like(values.type_id()) ||
          is_large_binary_like(values.type_id()))) {
      return Status::Invalid("Array is not primitive type: ", values.type()->ToString());
    }

    meta->type = ToFlatbufferType(values.type_id());

    ARROW_ASSIGN_OR_RAISE(meta->offset, stream_->Tell());

    meta->length = values.length();
    meta->null_count = values.null_count();
    meta->total_bytes = 0;

    return Status::OK();
  }

  template <typename ArrayType>
  Status WriteBinaryArray(const ArrayType& values, ArrayMetadata* meta,
                          const uint8_t** values_buffer, int64_t* values_bytes,
                          int64_t* bytes_written) {
    using offset_type = typename ArrayType::offset_type;

    int64_t offset_bytes = sizeof(offset_type) * (values.length() + 1);

    if (values.value_offsets()) {
      *values_bytes = values.raw_value_offsets()[values.length()];

      // Write the variable-length offsets
      RETURN_NOT_OK(WritePadded(
          stream_.get(), reinterpret_cast<const uint8_t*>(values.raw_value_offsets()),
          offset_bytes, bytes_written));
    } else {
      RETURN_NOT_OK(WritePaddedBlank(stream_.get(), offset_bytes, bytes_written));
    }
    meta->total_bytes += *bytes_written;

    if (values.value_data()) {
      *values_buffer = values.value_data()->data();
    }
    return Status::OK();
  }

  Status WriteArray(const Array& values, ArrayMetadata* meta) {
    RETURN_NOT_OK(CheckStarted());
    RETURN_NOT_OK(LoadArrayMetadata(values, meta));

    int64_t bytes_written;

    // Write the null bitmask
    if (values.null_count() > 0) {
      // We assume there is one bit for each value in values.nulls,
      // starting at the zero offset.
      int64_t null_bitmap_size = GetOutputLength(BitUtil::BytesForBits(values.length()));
      if (values.null_bitmap()) {
        auto null_bitmap = values.null_bitmap();
        RETURN_NOT_OK(WritePaddedWithOffset(stream_.get(), null_bitmap->data(),
                                            values.offset(), null_bitmap_size,
                                            &bytes_written));
      } else {
        RETURN_NOT_OK(WritePaddedBlank(stream_.get(), null_bitmap_size, &bytes_written));
      }
      meta->total_bytes += bytes_written;
    }

    int64_t values_bytes = 0;
    int64_t bit_offset = 0;

    const uint8_t* values_buffer = nullptr;

    if (is_binary_like(values.type_id())) {
      RETURN_NOT_OK(WriteBinaryArray(checked_cast<const BinaryArray&>(values), meta,
                                     &values_buffer, &values_bytes, &bytes_written));
    } else if (is_large_binary_like(values.type_id())) {
      RETURN_NOT_OK(WriteBinaryArray(checked_cast<const LargeBinaryArray&>(values), meta,
                                     &values_buffer, &values_bytes, &bytes_written));
    } else {
      const auto& prim_values = checked_cast<const PrimitiveArray&>(values);
      const auto& fw_type = checked_cast<const FixedWidthType&>(*values.type());

      values_bytes = BitUtil::BytesForBits(values.length() * fw_type.bit_width());

      if (prim_values.values()) {
        values_buffer = prim_values.values()->data() +
                        (prim_values.offset() * fw_type.bit_width() / 8);
        bit_offset = (prim_values.offset() * fw_type.bit_width()) % 8;
      }
    }
    if (values_buffer) {
      RETURN_NOT_OK(WritePaddedWithOffset(stream_.get(), values_buffer, bit_offset,
                                          values_bytes, &bytes_written));
    } else {
      RETURN_NOT_OK(WritePaddedBlank(stream_.get(), values_bytes, &bytes_written));
    }
    meta->total_bytes += bytes_written;

    return Status::OK();
  }

  Status WritePrimitiveValues(const Array& values) {
    // Prepare metadata payload
    ArrayMetadata meta;
    RETURN_NOT_OK(WriteArray(values, &meta));
    current_column_->SetValues(meta);
    return Status::OK();
  }

  Status Visit(const NullArray& values) override {
    std::shared_ptr<Array> sanitized_nulls;
    RETURN_NOT_OK(SanitizeUnsupportedTypes(values, &sanitized_nulls));
    return WritePrimitiveValues(*sanitized_nulls);
  }

#define VISIT_PRIMITIVE(TYPE) \
  Status Visit(const TYPE& values) override { return WritePrimitiveValues(values); }

  VISIT_PRIMITIVE(BooleanArray)
  VISIT_PRIMITIVE(Int8Array)
  VISIT_PRIMITIVE(Int16Array)
  VISIT_PRIMITIVE(Int32Array)
  VISIT_PRIMITIVE(Int64Array)
  VISIT_PRIMITIVE(UInt8Array)
  VISIT_PRIMITIVE(UInt16Array)
  VISIT_PRIMITIVE(UInt32Array)
  VISIT_PRIMITIVE(UInt64Array)
  VISIT_PRIMITIVE(FloatArray)
  VISIT_PRIMITIVE(DoubleArray)
  VISIT_PRIMITIVE(BinaryArray)
  VISIT_PRIMITIVE(StringArray)
  VISIT_PRIMITIVE(LargeBinaryArray)
  VISIT_PRIMITIVE(LargeStringArray)

#undef VISIT_PRIMITIVE

  Status Visit(const DictionaryArray& values) override {
    const auto& dict_type = checked_cast<const DictionaryType&>(*values.type());

    if (!is_integer(values.indices()->type_id())) {
      return Status::Invalid("Category values must be integers");
    }

    RETURN_NOT_OK(WritePrimitiveValues(*values.indices()));

    ArrayMetadata levels_meta;
    std::shared_ptr<Array> sanitized_dictionary;
    RETURN_NOT_OK(SanitizeUnsupportedTypes(*values.dictionary(), &sanitized_dictionary));
    RETURN_NOT_OK(WriteArray(*sanitized_dictionary, &levels_meta));
    current_column_->SetCategory(levels_meta, dict_type.ordered());
    return Status::OK();
  }

  Status Visit(const TimestampArray& values) override {
    RETURN_NOT_OK(WritePrimitiveValues(values));
    const auto& ts_type = checked_cast<const TimestampType&>(*values.type());
    current_column_->SetTimestamp(ts_type.unit(), ts_type.timezone());
    return Status::OK();
  }

  Status Visit(const Date32Array& values) override {
    RETURN_NOT_OK(WritePrimitiveValues(values));
    current_column_->SetDate();
    return Status::OK();
  }

  Status Visit(const Time32Array& values) override {
    RETURN_NOT_OK(WritePrimitiveValues(values));
    auto unit = checked_cast<const Time32Type&>(*values.type()).unit();
    current_column_->SetTime(unit);
    return Status::OK();
  }

  Status Visit(const Time64Array& values) override {
    return Status::NotImplemented("time64");
  }

  Status Append(const std::string& name, const Array& values) {
    current_column_ = metadata_.AddColumn(name);
    RETURN_NOT_OK(values.Accept(this));
    return current_column_->Finish();
  }

  Status Write(const Table& table) {
    for (int i = 0; i < table.num_columns(); ++i) {
      auto column = table.column(i);
      current_column_ = metadata_.AddColumn(table.field(i)->name());
      for (const auto chunk : column->chunks()) {
        RETURN_NOT_OK(chunk->Accept(this));
      }
      RETURN_NOT_OK(current_column_->Finish());
    }
    return Status::OK();
  }

 private:
  Status CheckStarted() {
    if (!initialized_stream_) {
      int64_t bytes_written_unused;
      RETURN_NOT_OK(WritePadded(stream_.get(),
                                reinterpret_cast<const uint8_t*>(kFeatherMagicBytes),
                                strlen(kFeatherMagicBytes), &bytes_written_unused));
      initialized_stream_ = true;
    }
    return Status::OK();
  }

  std::shared_ptr<io::OutputStream> stream_;

  bool initialized_stream_;
  TableBuilder metadata_;

  std::unique_ptr<ColumnBuilder> current_column_;

  Status AppendPrimitive(const PrimitiveArray& values, ArrayMetadata* out);
};

TableWriter::TableWriter() { impl_.reset(new TableWriterImpl()); }

TableWriter::~TableWriter() {}

Status TableWriter::Open(const std::shared_ptr<io::OutputStream>& stream,
                         std::unique_ptr<TableWriter>* out) {
  out->reset(new TableWriter());
  return (*out)->impl_->Open(stream);
}

void TableWriter::SetDescription(const std::string& desc) { impl_->SetDescription(desc); }

void TableWriter::SetNumRows(int64_t num_rows) { impl_->SetNumRows(num_rows); }

Status TableWriter::Append(const std::string& name, const Array& values) {
  return impl_->Append(name, values);
}

Status TableWriter::Write(const Table& table) { return impl_->Write(table); }

Status TableWriter::Finalize() { return impl_->Finalize(); }

}  // namespace feather
}  // namespace ipc
}  // namespace arrow
