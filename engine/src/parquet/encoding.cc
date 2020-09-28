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

#include "parquet/encoding.h"

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <immintrin.h>
#include "sboost/sboost.h"

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/stl.h"
#include "arrow/util/bit_stream_utils.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/hashing.h"
#include "arrow/util/logging.h"
#include "arrow/util/rle_encoding.h"
#include "arrow/util/ubsan.h"

#include "parquet/exception.h"
#include "parquet/platform.h"
#include "parquet/schema.h"
#include "parquet/types.h"

using arrow::Status;
using arrow::internal::checked_cast;

template<typename T>
using ArrowPoolVector = std::vector<T, ::arrow::stl::allocator<T>>;

namespace parquet {

    constexpr int64_t kInMemoryDefaultCapacity = 1024;

    class EncoderImpl : virtual public Encoder {
    public:
        EncoderImpl(const ColumnDescriptor *descr, Encoding::type encoding, MemoryPool *pool)
                : descr_(descr),
                  encoding_(encoding),
                  pool_(pool),
                  type_length_(descr ? descr->type_length() : -1) {}

        Encoding::type encoding() const override { return encoding_; }

        MemoryPool *memory_pool() const override { return pool_; }

    protected:
        // For accessing type-specific metadata, like FIXED_LEN_BYTE_ARRAY
        const ColumnDescriptor *descr_;
        const Encoding::type encoding_;
        MemoryPool *pool_;

        /// Type length from descr
        int type_length_;
    };

// ----------------------------------------------------------------------
// Plain encoder implementation

    template<typename DType>
    class PlainEncoder : public EncoderImpl, virtual public TypedEncoder<DType> {
    public:
        using T = typename DType::c_type;

        explicit PlainEncoder(const ColumnDescriptor *descr, MemoryPool *pool)
                : EncoderImpl(descr, Encoding::PLAIN, pool), sink_(pool) {}

        int64_t EstimatedDataEncodedSize() override { return sink_.length(); }

        std::shared_ptr<Buffer> FlushValues() override {
            std::shared_ptr<Buffer> buffer;
            PARQUET_THROW_NOT_OK(sink_.Finish(&buffer));
            return buffer;
        }

        using TypedEncoder<DType>::Put;

        void Put(const T *buffer, int num_values) override;

        void Put(const arrow::Array &values) override;

        void PutSpaced(const T *src, int num_values, const uint8_t *valid_bits,
                       int64_t valid_bits_offset) override {
            std::shared_ptr<ResizableBuffer> buffer;
            PARQUET_THROW_NOT_OK(arrow::AllocateResizableBuffer(this->memory_pool(),
                                                                num_values * sizeof(T), &buffer));
            int32_t num_valid_values = 0;
            arrow::internal::BitmapReader valid_bits_reader(valid_bits, valid_bits_offset,
                                                            num_values);
            T *data = reinterpret_cast<T *>(buffer->mutable_data());
            for (int32_t i = 0; i < num_values; i++) {
                if (valid_bits_reader.IsSet()) {
                    data[num_valid_values++] = src[i];
                }
                valid_bits_reader.Next();
            }
            Put(data, num_valid_values);
        }

        void UnsafePutByteArray(const void *data, uint32_t length) {
                    DCHECK(length == 0 || data != nullptr) << "Value ptr cannot be NULL";
            sink_.UnsafeAppend(&length, sizeof(uint32_t));
            sink_.UnsafeAppend(data, static_cast<int64_t>(length));
        }

        void Put(const ByteArray &val) {
            // Write the result to the output stream
            const int64_t increment = static_cast<int64_t>(val.len + sizeof(uint32_t));
            if (ARROW_PREDICT_FALSE(sink_.length() + increment > sink_.capacity())) {
                PARQUET_THROW_NOT_OK(sink_.Reserve(increment));
            }
            UnsafePutByteArray(val.ptr, val.len);
        }

    protected:
        arrow::BufferBuilder sink_;
    };

    template<typename DType>
    void PlainEncoder<DType>::Put(const T *buffer, int num_values) {
        if (num_values > 0) {
            PARQUET_THROW_NOT_OK(sink_.Append(buffer, num_values * sizeof(T)));
        }
    }

    template<>
    inline void PlainEncoder<ByteArrayType>::Put(const ByteArray *src, int num_values) {
        for (int i = 0; i < num_values; ++i) {
            Put(src[i]);
        }
    }

    template<typename ArrayType>
    void DirectPutImpl(const arrow::Array &values, arrow::BufferBuilder *sink) {
        if (values.type_id() != ArrayType::TypeClass::type_id) {
            std::string type_name = ArrayType::TypeClass::type_name();
            throw ParquetException("direct put to " + type_name + " from " +
                                   values.type()->ToString() + " not supported");
        }

        using value_type = typename ArrayType::value_type;
        constexpr auto value_size = sizeof(value_type);
        auto raw_values = checked_cast<const ArrayType &>(values).raw_values();

        if (values.null_count() == 0) {
            // no nulls, just dump the data
            PARQUET_THROW_NOT_OK(sink->Append(raw_values, values.length() * value_size));
        } else {
            PARQUET_THROW_NOT_OK(
                    sink->Reserve((values.length() - values.null_count()) * value_size));

            for (int64_t i = 0; i < values.length(); i++) {
                if (values.IsValid(i)) {
                    sink->UnsafeAppend(&raw_values[i], value_size);
                }
            }
        }
    }

    template<>
    void PlainEncoder<Int32Type>::Put(const arrow::Array &values) {
        DirectPutImpl<arrow::Int32Array>(values, &sink_);
    }

    template<>
    void PlainEncoder<Int64Type>::Put(const arrow::Array &values) {
        DirectPutImpl<arrow::Int64Array>(values, &sink_);
    }

    template<>
    void PlainEncoder<Int96Type>::Put(const arrow::Array &values) {
        ParquetException::NYI("direct put to Int96");
    }

    template<>
    void PlainEncoder<FloatType>::Put(const arrow::Array &values) {
        DirectPutImpl<arrow::FloatArray>(values, &sink_);
    }

    template<>
    void PlainEncoder<DoubleType>::Put(const arrow::Array &values) {
        DirectPutImpl<arrow::DoubleArray>(values, &sink_);
    }

    template<typename DType>
    void PlainEncoder<DType>::Put(const arrow::Array &values) {
        ParquetException::NYI("direct put of " + values.type()->ToString());
    }

    void AssertBinary(const arrow::Array &values) {
        if (values.type_id() != arrow::Type::BINARY &&
            values.type_id() != arrow::Type::STRING) {
            throw ParquetException("Only BinaryArray and subclasses supported");
        }
    }

    template<>
    inline void PlainEncoder<ByteArrayType>::Put(const arrow::Array &values) {
        AssertBinary(values);
        const auto &data = checked_cast<const arrow::BinaryArray &>(values);
        const int64_t total_bytes = data.value_offset(data.length()) - data.value_offset(0);
        PARQUET_THROW_NOT_OK(sink_.Reserve(total_bytes + data.length() * sizeof(uint32_t)));

        if (data.null_count() == 0) {
            // no nulls, just dump the data
            for (int64_t i = 0; i < data.length(); i++) {
                auto view = data.GetView(i);
                UnsafePutByteArray(view.data(), static_cast<uint32_t>(view.size()));
            }
        } else {
            for (int64_t i = 0; i < data.length(); i++) {
                if (data.IsValid(i)) {
                    auto view = data.GetView(i);
                    UnsafePutByteArray(view.data(), static_cast<uint32_t>(view.size()));
                }
            }
        }
    }

    void AssertFixedSizeBinary(const arrow::Array &values, int type_length) {
        if (values.type_id() != arrow::Type::FIXED_SIZE_BINARY &&
            values.type_id() != arrow::Type::DECIMAL) {
            throw ParquetException("Only FixedSizeBinaryArray and subclasses supported");
        }
        if (checked_cast<const arrow::FixedSizeBinaryType &>(*values.type()).byte_width() !=
            type_length) {
            throw ParquetException("Size mismatch: " + values.type()->ToString() +
                                   " should have been " + std::to_string(type_length) + " wide");
        }
    }

    template<>
    inline void PlainEncoder<FLBAType>::Put(const arrow::Array &values) {
        AssertFixedSizeBinary(values, descr_->type_length());
        const auto &data = checked_cast<const arrow::FixedSizeBinaryArray &>(values);

        if (data.null_count() == 0) {
            // no nulls, just dump the data
            PARQUET_THROW_NOT_OK(
                    sink_.Append(data.raw_values(), data.length() * data.byte_width()));
        } else {
            const int64_t total_bytes =
                    data.length() * data.byte_width() - data.null_count() * data.byte_width();
            PARQUET_THROW_NOT_OK(sink_.Reserve(total_bytes));
            for (int64_t i = 0; i < data.length(); i++) {
                if (data.IsValid(i)) {
                    sink_.UnsafeAppend(data.Value(i), data.byte_width());
                }
            }
        }
    }

    template<>
    inline void PlainEncoder<FLBAType>::Put(const FixedLenByteArray *src, int num_values) {
        if (descr_->type_length() == 0) {
            return;
        }
        for (int i = 0; i < num_values; ++i) {
            // Write the result to the output stream
                    DCHECK(src[i].ptr != nullptr) << "Value ptr cannot be NULL";
            PARQUET_THROW_NOT_OK(sink_.Append(src[i].ptr, descr_->type_length()));
        }
    }

    template<>
    class PlainEncoder<BooleanType> : public EncoderImpl, virtual public BooleanEncoder {
    public:
        explicit PlainEncoder(const ColumnDescriptor *descr, MemoryPool *pool)
                : EncoderImpl(descr, Encoding::PLAIN, pool),
                  bits_available_(kInMemoryDefaultCapacity * 8),
                  bits_buffer_(AllocateBuffer(pool, kInMemoryDefaultCapacity)),
                  sink_(pool),
                  bit_writer_(bits_buffer_->mutable_data(),
                              static_cast<int>(bits_buffer_->size())) {}

        int64_t EstimatedDataEncodedSize() override;

        std::shared_ptr<Buffer> FlushValues() override;

        void Put(const bool *src, int num_values) override;

        void Put(const std::vector<bool> &src, int num_values) override;

        void PutSpaced(const bool *src, int num_values, const uint8_t *valid_bits,
                       int64_t valid_bits_offset) override {
            std::shared_ptr<ResizableBuffer> buffer;
            PARQUET_THROW_NOT_OK(arrow::AllocateResizableBuffer(this->memory_pool(),
                                                                num_values * sizeof(T), &buffer));
            int32_t num_valid_values = 0;
            arrow::internal::BitmapReader valid_bits_reader(valid_bits, valid_bits_offset,
                                                            num_values);
            T *data = reinterpret_cast<T *>(buffer->mutable_data());
            for (int32_t i = 0; i < num_values; i++) {
                if (valid_bits_reader.IsSet()) {
                    data[num_valid_values++] = src[i];
                }
                valid_bits_reader.Next();
            }
            Put(data, num_valid_values);
        }

        void Put(const arrow::Array &values) override {
            if (values.type_id() != arrow::Type::BOOL) {
                throw ParquetException("direct put to boolean from " + values.type()->ToString() +
                                       " not supported");
            }

            const auto &data = checked_cast<const arrow::BooleanArray &>(values);
            if (data.null_count() == 0) {
                PARQUET_THROW_NOT_OK(sink_.Reserve(BitUtil::BytesForBits(data.length())));
                // no nulls, just dump the data
                arrow::internal::CopyBitmap(data.data()->GetValues<uint8_t>(1), data.offset(),
                                            data.length(), sink_.mutable_data(), sink_.length());
                sink_.UnsafeAdvance(data.length());
            } else {
                auto n_valid = BitUtil::BytesForBits(data.length() - data.null_count());
                PARQUET_THROW_NOT_OK(sink_.Reserve(n_valid));
                arrow::internal::FirstTimeBitmapWriter writer(sink_.mutable_data(), sink_.length(),
                                                              n_valid);

                for (int64_t i = 0; i < data.length(); i++) {
                    if (data.IsValid(i)) {
                        if (data.Value(i)) {
                            writer.Set();
                        } else {
                            writer.Clear();
                        }
                        writer.Next();
                    }
                }
                writer.Finish();
            }
        }

    private:
        int bits_available_;
        std::shared_ptr<ResizableBuffer> bits_buffer_;
        arrow::BufferBuilder sink_;
        arrow::BitUtil::BitWriter bit_writer_;

        template<typename SequenceType>
        void PutImpl(const SequenceType &src, int num_values);
    };

    template<typename SequenceType>
    void PlainEncoder<BooleanType>::PutImpl(const SequenceType &src, int num_values) {
        int bit_offset = 0;
        if (bits_available_ > 0) {
            int bits_to_write = std::min(bits_available_, num_values);
            for (int i = 0; i < bits_to_write; i++) {
                bit_writer_.PutValue(src[i], 1);
            }
            bits_available_ -= bits_to_write;
            bit_offset = bits_to_write;

            if (bits_available_ == 0) {
                bit_writer_.Flush();
                PARQUET_THROW_NOT_OK(
                        sink_.Append(bit_writer_.buffer(), bit_writer_.bytes_written()));
                bit_writer_.Clear();
            }
        }

        int bits_remaining = num_values - bit_offset;
        while (bit_offset < num_values) {
            bits_available_ = static_cast<int>(bits_buffer_->size()) * 8;

            int bits_to_write = std::min(bits_available_, bits_remaining);
            for (int i = bit_offset; i < bit_offset + bits_to_write; i++) {
                bit_writer_.PutValue(src[i], 1);
            }
            bit_offset += bits_to_write;
            bits_available_ -= bits_to_write;
            bits_remaining -= bits_to_write;

            if (bits_available_ == 0) {
                bit_writer_.Flush();
                PARQUET_THROW_NOT_OK(
                        sink_.Append(bit_writer_.buffer(), bit_writer_.bytes_written()));
                bit_writer_.Clear();
            }
        }
    }

    int64_t PlainEncoder<BooleanType>::EstimatedDataEncodedSize() {
        int64_t position = sink_.length();
        return position + bit_writer_.bytes_written();
    }

    std::shared_ptr<Buffer> PlainEncoder<BooleanType>::FlushValues() {
        if (bits_available_ > 0) {
            bit_writer_.Flush();
            PARQUET_THROW_NOT_OK(sink_.Append(bit_writer_.buffer(), bit_writer_.bytes_written()));
            bit_writer_.Clear();
            bits_available_ = static_cast<int>(bits_buffer_->size()) * 8;
        }

        std::shared_ptr<Buffer> buffer;
        PARQUET_THROW_NOT_OK(sink_.Finish(&buffer));
        return buffer;
    }

    void PlainEncoder<BooleanType>::Put(const bool *src, int num_values) {
        PutImpl(src, num_values);
    }

    void PlainEncoder<BooleanType>::Put(const std::vector<bool> &src, int num_values) {
        PutImpl(src, num_values);
    }

// ----------------------------------------------------------------------
// DictEncoder<T> implementations

    template<typename DType>
    struct DictEncoderTraits {
        using c_type = typename DType::c_type;
        using MemoTableType = arrow::internal::ScalarMemoTable<c_type>;
    };

    template<>
    struct DictEncoderTraits<ByteArrayType> {
        using MemoTableType = arrow::internal::BinaryMemoTable;
    };

    template<>
    struct DictEncoderTraits<FLBAType> {
        using MemoTableType = arrow::internal::BinaryMemoTable;
    };

// Initially 1024 elements
    static constexpr int32_t kInitialHashTableSize = 1 << 10;

/// See the dictionary encoding section of
/// https://github.com/Parquet/parquet-format.  The encoding supports
/// streaming encoding. Values are encoded as they are added while the
/// dictionary is being constructed. At any time, the buffered values
/// can be written out with the current dictionary size. More values
/// can then be added to the encoder, including new dictionary
/// entries.
    template<typename DType>
    class DictEncoderImpl : public EncoderImpl, virtual public DictEncoder<DType> {
        using MemoTableType = typename DictEncoderTraits<DType>::MemoTableType;

    public:
        typedef typename DType::c_type T;

        explicit DictEncoderImpl(const ColumnDescriptor *desc, MemoryPool *pool)
                : EncoderImpl(desc, Encoding::PLAIN_DICTIONARY, pool),
                  buffered_indices_(::arrow::stl::allocator<int32_t>(pool)),
                  dict_encoded_size_(0),
                  memo_table_(pool, kInitialHashTableSize) {}

        ~DictEncoderImpl() override { DCHECK(buffered_indices_.empty()); }

        int dict_encoded_size() override { return dict_encoded_size_; }

        int WriteIndices(uint8_t *buffer, int buffer_len) override {
            // Write bit width in first byte
            *buffer = static_cast<uint8_t>(bit_width());
            ++buffer;
            --buffer_len;

            arrow::util::RleEncoder encoder(buffer, buffer_len, bit_width());

            for (int32_t index : buffered_indices_) {
                if (!encoder.Put(index)) return -1;
            }
            encoder.Flush();

            ClearIndices();
            return 1 + encoder.len();
        }

        void set_type_length(int type_length) { this->type_length_ = type_length; }

        /// Returns a conservative estimate of the number of bytes needed to encode the buffered
        /// indices. Used to size the buffer passed to WriteIndices().
        int64_t EstimatedDataEncodedSize() override {
            // Note: because of the way RleEncoder::CheckBufferFull() is called, we have to
            // reserve
            // an extra "RleEncoder::MinBufferSize" bytes. These extra bytes won't be used
            // but not reserving them would cause the encoder to fail.
            return 1 +
                   arrow::util::RleEncoder::MaxBufferSize(
                           bit_width(), static_cast<int>(buffered_indices_.size())) +
                   arrow::util::RleEncoder::MinBufferSize(bit_width());
        }

        /// The minimum bit width required to encode the currently buffered indices.
        int bit_width() const override {
            if (ARROW_PREDICT_FALSE(num_entries() == 0)) return 0;
            if (ARROW_PREDICT_FALSE(num_entries() == 1)) return 1;
            return BitUtil::Log2(num_entries());
        }

        /// Encode value. Note that this does not actually write any data, just
        /// buffers the value's index to be written later.
        inline void Put(const T &value);

        // Not implemented for other data types
        inline void PutByteArray(const void *ptr, int32_t length);

        void Put(const T *src, int num_values) override {
            for (int32_t i = 0; i < num_values; i++) {
                Put(src[i]);
            }
        }

        void PutSpaced(const T *src, int num_values, const uint8_t *valid_bits,
                       int64_t valid_bits_offset) override {
            arrow::internal::BitmapReader valid_bits_reader(valid_bits, valid_bits_offset,
                                                            num_values);
            for (int32_t i = 0; i < num_values; i++) {
                if (valid_bits_reader.IsSet()) {
                    Put(src[i]);
                }
                valid_bits_reader.Next();
            }
        }

        using TypedEncoder<DType>::Put;

        void Put(const arrow::Array &values) override;

        void PutDictionary(const arrow::Array &values) override;

        template<typename ArrowType>
        void PutIndicesTyped(const arrow::Array &data) {
            using ArrayType = typename arrow::TypeTraits<ArrowType>::ArrayType;
            const auto &indices = checked_cast<const ArrayType &>(data);
            auto values = indices.raw_values();

            size_t buffer_position = buffered_indices_.size();
            buffered_indices_.resize(
                    buffer_position + static_cast<size_t>(indices.length() - indices.null_count()));
            if (indices.null_count() > 0) {
                arrow::internal::BitmapReader valid_bits_reader(indices.null_bitmap_data(),
                                                                indices.offset(), indices.length());
                for (int64_t i = 0; i < indices.length(); ++i) {
                    if (valid_bits_reader.IsSet()) {
                        buffered_indices_[buffer_position++] = static_cast<int32_t>(values[i]);
                    }
                    valid_bits_reader.Next();
                }
            } else {
                for (int64_t i = 0; i < indices.length(); ++i) {
                    buffered_indices_[buffer_position++] = static_cast<int32_t>(values[i]);
                }
            }
        }

        void PutIndices(const arrow::Array &data) override {
            switch (data.type()->id()) {
                case arrow::Type::INT8:
                    return PutIndicesTyped<arrow::Int8Type>(data);
                case arrow::Type::INT16:
                    return PutIndicesTyped<arrow::Int16Type>(data);
                case arrow::Type::INT32:
                    return PutIndicesTyped<arrow::Int32Type>(data);
                case arrow::Type::INT64:
                    return PutIndicesTyped<arrow::Int64Type>(data);
                default:
                    throw ParquetException("Dictionary indices were not signed integer");
            }
        }

        std::shared_ptr<Buffer> FlushValues() override {
            std::shared_ptr<ResizableBuffer> buffer =
                    AllocateBuffer(this->pool_, EstimatedDataEncodedSize());
            int result_size = WriteIndices(buffer->mutable_data(),
                                           static_cast<int>(EstimatedDataEncodedSize()));
            PARQUET_THROW_NOT_OK(buffer->Resize(result_size, false));
            return std::move(buffer);
        }

        /// Writes out the encoded dictionary to buffer. buffer must be preallocated to
        /// dict_encoded_size() bytes.
        void WriteDict(uint8_t *buffer) override;

        /// The number of entries in the dictionary.
        int num_entries() const override { return memo_table_.size(); }

    private:
        /// Clears all the indices (but leaves the dictionary).
        void ClearIndices() { buffered_indices_.clear(); }

        /// Indices that have not yet be written out by WriteIndices().
        ArrowPoolVector<int32_t> buffered_indices_;

        /// The number of bytes needed to encode the dictionary.
        int dict_encoded_size_;

        MemoTableType memo_table_;
    };

    template<typename DType>
    void DictEncoderImpl<DType>::WriteDict(uint8_t *buffer) {
        // For primitive types, only a memcpy
                DCHECK_EQ(static_cast<size_t>(dict_encoded_size_), sizeof(T) * memo_table_.size());
        memo_table_.CopyValues(0 /* start_pos */, reinterpret_cast<T *>(buffer));
    }

// ByteArray and FLBA already have the dictionary encoded in their data heaps
    template<>
    void DictEncoderImpl<ByteArrayType>::WriteDict(uint8_t *buffer) {
        memo_table_.VisitValues(0, [&buffer](const arrow::util::string_view &v) {
            uint32_t len = static_cast<uint32_t>(v.length());
            memcpy(buffer, &len, sizeof(len));
            buffer += sizeof(len);
            memcpy(buffer, v.data(), len);
            buffer += len;
        });
    }

    template<>
    void DictEncoderImpl<FLBAType>::WriteDict(uint8_t *buffer) {
        memo_table_.VisitValues(0, [&](const arrow::util::string_view &v) {
                    DCHECK_EQ(v.length(), static_cast<size_t>(type_length_));
            memcpy(buffer, v.data(), type_length_);
            buffer += type_length_;
        });
    }

    template<typename DType>
    inline void DictEncoderImpl<DType>::Put(const T &v) {
        // Put() implementation for primitive types
        auto on_found = [](int32_t memo_index) {};
        auto on_not_found = [this](int32_t memo_index) {
            dict_encoded_size_ += static_cast<int>(sizeof(T));
        };

        auto memo_index = memo_table_.GetOrInsert(v, on_found, on_not_found);
        buffered_indices_.push_back(memo_index);
    }

    template<typename DType>
    inline void DictEncoderImpl<DType>::PutByteArray(const void *ptr, int32_t length) {
                DCHECK(false);
    }

    template<>
    inline void DictEncoderImpl<ByteArrayType>::PutByteArray(const void *ptr,
                                                             int32_t length) {
        static const uint8_t empty[] = {0};

        auto on_found = [](int32_t memo_index) {};
        auto on_not_found = [&](int32_t memo_index) {
            dict_encoded_size_ += static_cast<int>(length + sizeof(uint32_t));
        };

                DCHECK(ptr != nullptr || length == 0);
        ptr = (ptr != nullptr) ? ptr : empty;
        auto memo_index = memo_table_.GetOrInsert(ptr, length, on_found, on_not_found);
        buffered_indices_.push_back(memo_index);
    }

    template<>
    inline void DictEncoderImpl<ByteArrayType>::Put(const ByteArray &val) {
        return PutByteArray(val.ptr, static_cast<int32_t>(val.len));
    }

    template<>
    inline void DictEncoderImpl<FLBAType>::Put(const FixedLenByteArray &v) {
        static const uint8_t empty[] = {0};

        auto on_found = [](int32_t memo_index) {};
        auto on_not_found = [this](int32_t memo_index) { dict_encoded_size_ += type_length_; };

                DCHECK(v.ptr != nullptr || type_length_ == 0);
        const void *ptr = (v.ptr != nullptr) ? v.ptr : empty;
        auto memo_index = memo_table_.GetOrInsert(ptr, type_length_, on_found, on_not_found);
        buffered_indices_.push_back(memo_index);
    }

    template<>
    void DictEncoderImpl<Int96Type>::Put(const arrow::Array &values) {
        ParquetException::NYI("Direct put to Int96");
    }

    template<>
    void DictEncoderImpl<Int96Type>::PutDictionary(const arrow::Array &values) {
        ParquetException::NYI("Direct put to Int96");
    }

    template<typename DType>
    void DictEncoderImpl<DType>::Put(const arrow::Array &values) {
        using ArrayType = typename arrow::CTypeTraits<typename DType::c_type>::ArrayType;
        const auto &data = checked_cast<const ArrayType &>(values);
        if (data.null_count() == 0) {
            // no nulls, just dump the data
            for (int64_t i = 0; i < data.length(); i++) {
                Put(data.Value(i));
            }
        } else {
            for (int64_t i = 0; i < data.length(); i++) {
                if (data.IsValid(i)) {
                    Put(data.Value(i));
                }
            }
        }
    }

    template<>
    void DictEncoderImpl<FLBAType>::Put(const arrow::Array &values) {
        AssertFixedSizeBinary(values, type_length_);
        const auto &data = checked_cast<const arrow::FixedSizeBinaryArray &>(values);
        if (data.null_count() == 0) {
            // no nulls, just dump the data
            for (int64_t i = 0; i < data.length(); i++) {
                Put(FixedLenByteArray(data.Value(i)));
            }
        } else {
            std::vector<uint8_t> empty(type_length_, 0);
            for (int64_t i = 0; i < data.length(); i++) {
                if (data.IsValid(i)) {
                    Put(FixedLenByteArray(data.Value(i)));
                }
            }
        }
    }

    template<>
    void DictEncoderImpl<ByteArrayType>::Put(const arrow::Array &values) {
        AssertBinary(values);
        const auto &data = checked_cast<const arrow::BinaryArray &>(values);
        if (data.null_count() == 0) {
            // no nulls, just dump the data
            for (int64_t i = 0; i < data.length(); i++) {
                auto view = data.GetView(i);
                PutByteArray(view.data(), static_cast<int32_t>(view.size()));
            }
        } else {
            for (int64_t i = 0; i < data.length(); i++) {
                if (data.IsValid(i)) {
                    auto view = data.GetView(i);
                    PutByteArray(view.data(), static_cast<int32_t>(view.size()));
                }
            }
        }
    }

    template<typename DType>
    void AssertCanPutDictionary(DictEncoderImpl<DType> *encoder, const arrow::Array &dict) {
        if (dict.null_count() > 0) {
            throw ParquetException("Inserted dictionary cannot cannot contain nulls");
        }

        if (encoder->num_entries() > 0) {
            throw ParquetException("Can only call PutDictionary on an empty DictEncoder");
        }
    }

    template<typename DType>
    void DictEncoderImpl<DType>::PutDictionary(const arrow::Array &values) {
        AssertCanPutDictionary(this, values);

        using ArrayType = typename arrow::CTypeTraits<typename DType::c_type>::ArrayType;
        const auto &data = checked_cast<const ArrayType &>(values);

        dict_encoded_size_ += static_cast<int>(sizeof(typename DType::c_type) * data.length());
        for (int64_t i = 0; i < data.length(); i++) {
            ARROW_IGNORE_EXPR(
                    memo_table_.GetOrInsert(data.Value(i),
                            /*on_found=*/[](int32_t memo_index) {},
                            /*on_not_found=*/[](int32_t memo_index) {}));
        }
    }

    template<>
    void DictEncoderImpl<FLBAType>::PutDictionary(const arrow::Array &values) {
        AssertFixedSizeBinary(values, type_length_);
        AssertCanPutDictionary(this, values);

        const auto &data = checked_cast<const arrow::FixedSizeBinaryArray &>(values);

        dict_encoded_size_ += static_cast<int>(type_length_ * data.length());
        for (int64_t i = 0; i < data.length(); i++) {
            ARROW_IGNORE_EXPR(
                    memo_table_.GetOrInsert(data.Value(i), type_length_,
                            /*on_found=*/[](int32_t memo_index) {},
                            /*on_not_found=*/[](int32_t memo_index) {}));
        }
    }

    template<>
    void DictEncoderImpl<ByteArrayType>::PutDictionary(const arrow::Array &values) {
        AssertBinary(values);
        AssertCanPutDictionary(this, values);

        const auto &data = checked_cast<const arrow::BinaryArray &>(values);

        for (int64_t i = 0; i < data.length(); i++) {
            auto v = data.GetView(i);
            dict_encoded_size_ += static_cast<int>(v.size() + sizeof(uint32_t));
            ARROW_IGNORE_EXPR(
                    memo_table_.GetOrInsert(v.data(), static_cast<int32_t>(v.size()),
                            /*on_found=*/[](int32_t memo_index) {},
                            /*on_not_found=*/[](int32_t memo_index) {}));
        }
    }

// ----------------------------------------------------------------------
// Encoder and decoder factory functions

    std::unique_ptr<Encoder> MakeEncoder(Type::type type_num, Encoding::type encoding,
                                         bool use_dictionary, const ColumnDescriptor *descr,
                                         MemoryPool *pool) {
        if (use_dictionary) {
            switch (type_num) {
                case Type::INT32:
                    return std::unique_ptr<Encoder>(new DictEncoderImpl<Int32Type>(descr, pool));
                case Type::INT64:
                    return std::unique_ptr<Encoder>(new DictEncoderImpl<Int64Type>(descr, pool));
                case Type::INT96:
                    return std::unique_ptr<Encoder>(new DictEncoderImpl<Int96Type>(descr, pool));
                case Type::FLOAT:
                    return std::unique_ptr<Encoder>(new DictEncoderImpl<FloatType>(descr, pool));
                case Type::DOUBLE:
                    return std::unique_ptr<Encoder>(new DictEncoderImpl<DoubleType>(descr, pool));
                case Type::BYTE_ARRAY:
                    return std::unique_ptr<Encoder>(new DictEncoderImpl<ByteArrayType>(descr, pool));
                case Type::FIXED_LEN_BYTE_ARRAY:
                    return std::unique_ptr<Encoder>(new DictEncoderImpl<FLBAType>(descr, pool));
                default:
                            DCHECK(false) << "Encoder not implemented";
                    break;
            }
        } else if (encoding == Encoding::PLAIN) {
            switch (type_num) {
                case Type::BOOLEAN:
                    return std::unique_ptr<Encoder>(new PlainEncoder<BooleanType>(descr, pool));
                case Type::INT32:
                    return std::unique_ptr<Encoder>(new PlainEncoder<Int32Type>(descr, pool));
                case Type::INT64:
                    return std::unique_ptr<Encoder>(new PlainEncoder<Int64Type>(descr, pool));
                case Type::INT96:
                    return std::unique_ptr<Encoder>(new PlainEncoder<Int96Type>(descr, pool));
                case Type::FLOAT:
                    return std::unique_ptr<Encoder>(new PlainEncoder<FloatType>(descr, pool));
                case Type::DOUBLE:
                    return std::unique_ptr<Encoder>(new PlainEncoder<DoubleType>(descr, pool));
                case Type::BYTE_ARRAY:
                    return std::unique_ptr<Encoder>(new PlainEncoder<ByteArrayType>(descr, pool));
                case Type::FIXED_LEN_BYTE_ARRAY:
                    return std::unique_ptr<Encoder>(new PlainEncoder<FLBAType>(descr, pool));
                default:
                            DCHECK(false) << "Encoder not implemented";
                    break;
            }
        } else {
            ParquetException::NYI("Selected encoding is not supported");
        }
                DCHECK(false) << "Should not be able to reach this code";
        return nullptr;
    }

    class DecoderImpl : virtual public Decoder {
    public:
        void SetData(int num_values, const uint8_t *data, int len) override {
            num_values_ = num_values;
            data_ = data;
            len_ = len;
        }

        int values_left() const override { return num_values_; }

        Encoding::type encoding() const override { return encoding_; }

    protected:
        explicit DecoderImpl(const ColumnDescriptor *descr, Encoding::type encoding)
                : descr_(descr), encoding_(encoding), num_values_(0), data_(NULLPTR), len_(0) {}

        // For accessing type-specific metadata, like FIXED_LEN_BYTE_ARRAY
        const ColumnDescriptor *descr_;

        const Encoding::type encoding_;
        int num_values_;
        const uint8_t *data_;
        int len_;
        int type_length_;
    };

    template<typename DType>
    class PlainDecoder : public DecoderImpl, virtual public TypedDecoder<DType> {
    public:
        using T = typename DType::c_type;

        explicit PlainDecoder(const ColumnDescriptor *descr);

        int Decode(T *buffer, int max_values) override;

        int Skip(int max_values) override;

        int DecodeArrow(int num_values, int null_count, const uint8_t *valid_bits,
                        int64_t valid_bits_offset,
                        typename EncodingTraits<DType>::Accumulator *builder) override;

        int DecodeArrow(int num_values, int null_count, const uint8_t *valid_bits,
                        int64_t valid_bits_offset,
                        typename EncodingTraits<DType>::DictAccumulator *builder) override;
    };

    template<>
    inline int PlainDecoder<Int96Type>::DecodeArrow(
            int num_values, int null_count, const uint8_t *valid_bits, int64_t valid_bits_offset,
            typename EncodingTraits<Int96Type>::Accumulator *builder) {
        ParquetException::NYI("DecodeArrow not supported for Int96");
    }

    template<>
    inline int PlainDecoder<Int96Type>::DecodeArrow(
            int num_values, int null_count, const uint8_t *valid_bits, int64_t valid_bits_offset,
            typename EncodingTraits<Int96Type>::DictAccumulator *builder) {
        ParquetException::NYI("DecodeArrow not supported for Int96");
    }

    template<>
    inline int PlainDecoder<BooleanType>::DecodeArrow(
            int num_values, int null_count, const uint8_t *valid_bits, int64_t valid_bits_offset,
            typename EncodingTraits<BooleanType>::DictAccumulator *builder) {
        ParquetException::NYI("dictionaries of BooleanType");
    }

    template<typename DType>
    int PlainDecoder<DType>::DecodeArrow(
            int num_values, int null_count, const uint8_t *valid_bits, int64_t valid_bits_offset,
            typename EncodingTraits<DType>::Accumulator *builder) {
        using value_type = typename DType::c_type;

        constexpr int value_size = static_cast<int>(sizeof(value_type));
        int values_decoded = num_values - null_count;
        if (ARROW_PREDICT_FALSE(len_ < value_size * values_decoded)) {
            ParquetException::EofException();
        }

        PARQUET_THROW_NOT_OK(builder->Reserve(num_values));

        arrow::internal::BitmapReader bit_reader(valid_bits, valid_bits_offset, num_values);
        for (int i = 0; i < num_values; ++i) {
            if (bit_reader.IsSet()) {
                builder->UnsafeAppend(arrow::util::SafeLoadAs<value_type>(data_));
                data_ += sizeof(value_type);
            } else {
                builder->UnsafeAppendNull();
            }
            bit_reader.Next();
        }

        num_values_ -= values_decoded;
        len_ -= sizeof(value_type) * values_decoded;
        return values_decoded;
    }

    template<typename DType>
    int PlainDecoder<DType>::DecodeArrow(
            int num_values, int null_count, const uint8_t *valid_bits, int64_t valid_bits_offset,
            typename EncodingTraits<DType>::DictAccumulator *builder) {
        using value_type = typename DType::c_type;

        constexpr int value_size = static_cast<int>(sizeof(value_type));
        int values_decoded = num_values - null_count;
        if (ARROW_PREDICT_FALSE(len_ < value_size * values_decoded)) {
            ParquetException::EofException();
        }

        PARQUET_THROW_NOT_OK(builder->Reserve(num_values));

        arrow::internal::BitmapReader bit_reader(valid_bits, valid_bits_offset, num_values);
        for (int i = 0; i < num_values; ++i) {
            if (bit_reader.IsSet()) {
                PARQUET_THROW_NOT_OK(builder->Append(arrow::util::SafeLoadAs<value_type>(data_)));
                data_ += sizeof(value_type);
            } else {
                PARQUET_THROW_NOT_OK(builder->AppendNull());
            }
            bit_reader.Next();
        }

        num_values_ -= values_decoded;
        len_ -= sizeof(value_type) * values_decoded;
        return values_decoded;
    }

// Decode routine templated on C++ type rather than type enum
    template<typename T>
    inline int DecodePlain(const uint8_t *data, int64_t data_size, int num_values,
                           int type_length, T *out) {
        int bytes_to_decode = num_values * static_cast<int>(sizeof(T));
        if (data_size < bytes_to_decode) {
            ParquetException::EofException();
        }
        // If bytes_to_decode == 0, data could be null
        if (bytes_to_decode > 0) {
            memcpy(out, data, bytes_to_decode);
        }
        return bytes_to_decode;
    }

    template<typename DType>
    PlainDecoder<DType>::PlainDecoder(const ColumnDescriptor *descr)
            : DecoderImpl(descr, Encoding::PLAIN) {
        if (descr_ && descr_->physical_type() == Type::FIXED_LEN_BYTE_ARRAY) {
            type_length_ = descr_->type_length();
        } else {
            type_length_ = -1;
        }
    }

// Template specialization for BYTE_ARRAY. The written values do not own their
// own data.
    template<>
    inline int DecodePlain<ByteArray>(const uint8_t *data, int64_t data_size, int num_values,
                                      int type_length, ByteArray *out) {
        int bytes_decoded = 0;
        int increment;
        for (int i = 0; i < num_values; ++i) {
            uint32_t len = out[i].len = arrow::util::SafeLoadAs<uint32_t>(data);
            increment = static_cast<int>(sizeof(uint32_t) + len);
            if (data_size < increment) ParquetException::EofException();
            out[i].ptr = data + sizeof(uint32_t);
            data += increment;
            data_size -= increment;
            bytes_decoded += increment;
        }
        return bytes_decoded;
    }

// Template specialization for FIXED_LEN_BYTE_ARRAY. The written values do not
// own their own data.
    template<>
    inline int DecodePlain<FixedLenByteArray>(const uint8_t *data, int64_t data_size,
                                              int num_values, int type_length,
                                              FixedLenByteArray *out) {
        int bytes_to_decode = type_length * num_values;
        if (data_size < bytes_to_decode) {
            ParquetException::EofException();
        }
        for (int i = 0; i < num_values; ++i) {
            out[i].ptr = data;
            data += type_length;
            data_size -= type_length;
        }
        return bytes_to_decode;
    }

    template<typename DType>
    int PlainDecoder<DType>::Decode(T *buffer, int max_values) {
        max_values = std::min(max_values, num_values_);
        int bytes_consumed = DecodePlain<T>(data_, len_, max_values, type_length_, buffer);
        data_ += bytes_consumed;
        len_ -= bytes_consumed;
        num_values_ -= max_values;
        return max_values;
    }

// Decode routine templated on C++ type rather than type enum
    template<typename T>
    inline int SkipPlain(const uint8_t *data, int64_t data_size, int num_values,
                         int type_length) {
        return num_values * static_cast<int>(sizeof(T));
    }

    template<>
    inline int SkipPlain<ByteArray>(const uint8_t *data, int64_t data_size, int num_values,
                                    int type_length) {
        int bytes_decoded = 0;
        int increment;
        for (int i = 0; i < num_values; ++i) {
            uint32_t len = arrow::util::SafeLoadAs<uint32_t>(data);
            increment = static_cast<int>(sizeof(uint32_t) + len);
            if (data_size < increment) ParquetException::EofException();
            data += increment;
            data_size -= increment;
            bytes_decoded += increment;
        }
        return bytes_decoded;
    }

    template<>
    inline int SkipPlain<FixedLenByteArray>(const uint8_t *data, int64_t data_size, int num_values,
                                            int type_length) {
        return type_length * num_values;
    }

    template<typename DType>
    int PlainDecoder<DType>::Skip(int max_values) {
        max_values = std::min(max_values, num_values_);
        int bytes_skipped = SkipPlain<T>(data_, len_, max_values, type_length_);
        data_ += bytes_skipped;
        len_ -= bytes_skipped;
        num_values_ -= max_values;
        return max_values;
    }


    class PlainBooleanDecoder : public DecoderImpl,
                                virtual public TypedDecoder<BooleanType>,
                                virtual public BooleanDecoder {
    public:
        explicit PlainBooleanDecoder(const ColumnDescriptor *descr);

        void SetData(int num_values, const uint8_t *data, int len) override;

        // Two flavors of bool decoding
        int Decode(uint8_t *buffer, int max_values) override;

        int Skip(int max_values) override;

        int Decode(bool *buffer, int max_values) override;

        int DecodeArrow(int num_values, int null_count, const uint8_t *valid_bits,
                        int64_t valid_bits_offset,
                        typename EncodingTraits<BooleanType>::Accumulator *out) override;

        int DecodeArrow(int num_values, int null_count, const uint8_t *valid_bits,
                        int64_t valid_bits_offset,
                        typename EncodingTraits<BooleanType>::DictAccumulator *out) override;

    private:
        std::unique_ptr<arrow::BitUtil::BitReader> bit_reader_;
    };

    PlainBooleanDecoder::PlainBooleanDecoder(const ColumnDescriptor *descr)
            : DecoderImpl(descr, Encoding::PLAIN) {}

    void PlainBooleanDecoder::SetData(int num_values, const uint8_t *data, int len) {
        num_values_ = num_values;
        bit_reader_.reset(new BitUtil::BitReader(data, len));
    }

    int PlainBooleanDecoder::DecodeArrow(
            int num_values, int null_count, const uint8_t *valid_bits, int64_t valid_bits_offset,
            typename EncodingTraits<BooleanType>::Accumulator *builder) {
        int values_decoded = num_values - null_count;
        if (ARROW_PREDICT_FALSE(num_values_ < values_decoded)) {
            ParquetException::EofException();
        }

        PARQUET_THROW_NOT_OK(builder->Reserve(num_values));

        arrow::internal::BitmapReader valid_reader(valid_bits, valid_bits_offset, num_values);
        for (int i = 0; i < num_values; ++i) {
            if (valid_reader.IsSet()) {
                bool value;
                ARROW_IGNORE_EXPR(bit_reader_->GetValue(1, &value));
                builder->UnsafeAppend(value);
            } else {
                builder->UnsafeAppendNull();
            }
            valid_reader.Next();
        }

        num_values_ -= values_decoded;
        return values_decoded;
    }

    inline int PlainBooleanDecoder::DecodeArrow(
            int num_values, int null_count, const uint8_t *valid_bits, int64_t valid_bits_offset,
            typename EncodingTraits<BooleanType>::DictAccumulator *builder) {
        ParquetException::NYI("dictionaries of BooleanType");
    }

    int PlainBooleanDecoder::Decode(uint8_t *buffer, int max_values) {
        max_values = std::min(max_values, num_values_);
        bool val;
        arrow::internal::BitmapWriter bit_writer(buffer, 0, max_values);
        for (int i = 0; i < max_values; ++i) {
            if (!bit_reader_->GetValue(1, &val)) {
                ParquetException::EofException();
            }
            if (val) {
                bit_writer.Set();
            }
            bit_writer.Next();
        }
        bit_writer.Finish();
        num_values_ -= max_values;
        return max_values;
    }

    int PlainBooleanDecoder::Skip(int max_values) {
        max_values = std::min(max_values, num_values_);
        bit_reader_->Skip(1, max_values);
        num_values_ -= max_values;
        return max_values;
    }

    int PlainBooleanDecoder::Decode(bool *buffer, int max_values) {
        max_values = std::min(max_values, num_values_);
        if (bit_reader_->GetBatch(1, buffer, max_values) != max_values) {
            ParquetException::EofException();
        }
        num_values_ -= max_values;
        return max_values;
    }

    struct ArrowBinaryHelper {
        explicit ArrowBinaryHelper(typename EncodingTraits<ByteArrayType>::Accumulator *out) {
            this->out = out;
            this->builder = out->builder.get();
            this->chunk_space_remaining =
                    ::arrow::kBinaryMemoryLimit - this->builder->value_data_length();
        }

        Status PushChunk() {
            std::shared_ptr<::arrow::Array> result;
            RETURN_NOT_OK(builder->Finish(&result));
            out->chunks.push_back(result);
            chunk_space_remaining = ::arrow::kBinaryMemoryLimit;
            return Status::OK();
        }

        bool CanFit(int64_t length) const { return length <= chunk_space_remaining; }

        void UnsafeAppend(const uint8_t *data, int32_t length) {
            chunk_space_remaining -= length;
            builder->UnsafeAppend(data, length);
        }

        void UnsafeAppendNull() { builder->UnsafeAppendNull(); }

        Status Append(const uint8_t *data, int32_t length) {
            chunk_space_remaining -= length;
            return builder->Append(data, length);
        }

        Status AppendNull() { return builder->AppendNull(); }

        typename EncodingTraits<ByteArrayType>::Accumulator *out;
        arrow::BinaryBuilder *builder;
        int64_t chunk_space_remaining;
    };

    template<>
    inline int PlainDecoder<ByteArrayType>::DecodeArrow(
            int num_values, int null_count, const uint8_t *valid_bits, int64_t valid_bits_offset,
            typename EncodingTraits<ByteArrayType>::Accumulator *builder) {
        ParquetException::NYI();
    }

    template<>
    inline int PlainDecoder<ByteArrayType>::DecodeArrow(
            int num_values, int null_count, const uint8_t *valid_bits, int64_t valid_bits_offset,
            typename EncodingTraits<ByteArrayType>::DictAccumulator *builder) {
        ParquetException::NYI();
    }

    template<>
    inline int PlainDecoder<FLBAType>::DecodeArrow(
            int num_values, int null_count, const uint8_t *valid_bits, int64_t valid_bits_offset,
            typename EncodingTraits<FLBAType>::Accumulator *builder) {
        int values_decoded = num_values - null_count;
        if (ARROW_PREDICT_FALSE(len_ < descr_->type_length() * values_decoded)) {
            ParquetException::EofException();
        }

        PARQUET_THROW_NOT_OK(builder->Reserve(num_values));

        arrow::internal::BitmapReader bit_reader(valid_bits, valid_bits_offset, num_values);
        for (int i = 0; i < num_values; ++i) {
            if (bit_reader.IsSet()) {
                builder->UnsafeAppend(data_);
                data_ += descr_->type_length();
            } else {
                builder->UnsafeAppendNull();
            }
            bit_reader.Next();
        }

        num_values_ -= values_decoded;
        len_ -= descr_->type_length() * values_decoded;
        return values_decoded;
    }

    template<>
    inline int PlainDecoder<FLBAType>::DecodeArrow(
            int num_values, int null_count, const uint8_t *valid_bits, int64_t valid_bits_offset,
            typename EncodingTraits<FLBAType>::DictAccumulator *builder) {
        int values_decoded = num_values - null_count;
        if (ARROW_PREDICT_FALSE(len_ < descr_->type_length() * values_decoded)) {
            ParquetException::EofException();
        }

        PARQUET_THROW_NOT_OK(builder->Reserve(num_values));

        arrow::internal::BitmapReader bit_reader(valid_bits, valid_bits_offset, num_values);
        for (int i = 0; i < num_values; ++i) {
            if (bit_reader.IsSet()) {
                PARQUET_THROW_NOT_OK(builder->Append(data_));
                data_ += descr_->type_length();
            } else {
                PARQUET_THROW_NOT_OK(builder->AppendNull());
            }
            bit_reader.Next();
        }

        num_values_ -= values_decoded;
        len_ -= descr_->type_length() * values_decoded;
        return values_decoded;
    }

    class PlainByteArrayDecoder : public PlainDecoder<ByteArrayType>,
                                  virtual public ByteArrayDecoder {
    public:
        using Base = PlainDecoder<ByteArrayType>;
        using Base::DecodeSpaced;
        using Base::PlainDecoder;

        // ----------------------------------------------------------------------
        // Dictionary read paths

        int DecodeArrow(int num_values, int null_count, const uint8_t *valid_bits,
                        int64_t valid_bits_offset,
                        arrow::BinaryDictionary32Builder *builder) override {
            int result = 0;
            PARQUET_THROW_NOT_OK(DecodeArrow(num_values, null_count, valid_bits,
                                             valid_bits_offset, builder, &result));
            return result;
        }

        int DecodeArrowNonNull(int num_values,
                               arrow::BinaryDictionary32Builder *builder) override {
            int result = 0;
            PARQUET_THROW_NOT_OK(DecodeArrowNonNull(num_values, builder, &result));
            return result;
        }

        // ----------------------------------------------------------------------
        // Optimized dense binary read paths

        int DecodeArrow(int num_values, int null_count, const uint8_t *valid_bits,
                        int64_t valid_bits_offset,
                        typename EncodingTraits<ByteArrayType>::Accumulator *out) override {
            int result = 0;
            PARQUET_THROW_NOT_OK(DecodeArrowDense(num_values, null_count, valid_bits,
                                                  valid_bits_offset, out, &result));
            return result;
        }

        int DecodeArrowNonNull(
                int num_values, typename EncodingTraits<ByteArrayType>::Accumulator *out) override {
            int result = 0;
            PARQUET_THROW_NOT_OK(DecodeArrowDenseNonNull(num_values, out, &result));
            return result;
        }

    private:
        Status DecodeArrowDense(int num_values, int null_count, const uint8_t *valid_bits,
                                int64_t valid_bits_offset,
                                typename EncodingTraits<ByteArrayType>::Accumulator *out,
                                int *out_values_decoded) {
            ArrowBinaryHelper helper(out);
            arrow::internal::BitmapReader bit_reader(valid_bits, valid_bits_offset, num_values);
            int values_decoded = 0;

            RETURN_NOT_OK(helper.builder->Reserve(num_values));
            RETURN_NOT_OK(helper.builder->ReserveData(
                    std::min<int64_t>(len_, helper.chunk_space_remaining)));
            for (int i = 0; i < num_values; ++i) {
                if (bit_reader.IsSet()) {
                    auto value_len = static_cast<int32_t>(arrow::util::SafeLoadAs<uint32_t>(data_));
                    int increment = static_cast<int>(sizeof(uint32_t) + value_len);
                    if (ARROW_PREDICT_FALSE(len_ < increment)) ParquetException::EofException();
                    if (ARROW_PREDICT_FALSE(!helper.CanFit(value_len))) {
                        // This element would exceed the capacity of a chunk
                        RETURN_NOT_OK(helper.PushChunk());
                        RETURN_NOT_OK(helper.builder->Reserve(num_values - i));
                        RETURN_NOT_OK(helper.builder->ReserveData(
                                std::min<int64_t>(len_, helper.chunk_space_remaining)));
                    }
                    helper.UnsafeAppend(data_ + sizeof(uint32_t), value_len);
                    data_ += increment;
                    len_ -= increment;
                    ++values_decoded;
                } else {
                    helper.UnsafeAppendNull();
                }
                bit_reader.Next();
            }

            num_values_ -= values_decoded;
            *out_values_decoded = values_decoded;
            return Status::OK();
        }

        Status DecodeArrowDenseNonNull(int num_values,
                                       typename EncodingTraits<ByteArrayType>::Accumulator *out,
                                       int *values_decoded) {
            ArrowBinaryHelper helper(out);
            num_values = std::min(num_values, num_values_);
            RETURN_NOT_OK(helper.builder->Reserve(num_values));
            RETURN_NOT_OK(helper.builder->ReserveData(
                    std::min<int64_t>(len_, helper.chunk_space_remaining)));
            for (int i = 0; i < num_values; ++i) {
                int32_t value_len = static_cast<int32_t>(arrow::util::SafeLoadAs<uint32_t>(data_));
                int increment = static_cast<int>(sizeof(uint32_t) + value_len);
                if (ARROW_PREDICT_FALSE(len_ < increment)) ParquetException::EofException();
                if (ARROW_PREDICT_FALSE(!helper.CanFit(value_len))) {
                    // This element would exceed the capacity of a chunk
                    RETURN_NOT_OK(helper.PushChunk());
                    RETURN_NOT_OK(helper.builder->Reserve(num_values - i));
                    RETURN_NOT_OK(helper.builder->ReserveData(
                            std::min<int64_t>(len_, helper.chunk_space_remaining)));
                }
                helper.UnsafeAppend(data_ + sizeof(uint32_t), value_len);
                data_ += increment;
                len_ -= increment;
            }

            num_values_ -= num_values;
            *values_decoded = num_values;
            return Status::OK();
        }

        template<typename BuilderType>
        Status DecodeArrow(int num_values, int null_count, const uint8_t *valid_bits,
                           int64_t valid_bits_offset, BuilderType *builder,
                           int *out_values_decoded) {
            RETURN_NOT_OK(builder->Reserve(num_values));
            arrow::internal::BitmapReader bit_reader(valid_bits, valid_bits_offset, num_values);
            int values_decoded = 0;
            for (int i = 0; i < num_values; ++i) {
                if (bit_reader.IsSet()) {
                    uint32_t value_len = arrow::util::SafeLoadAs<uint32_t>(data_);
                    int increment = static_cast<int>(sizeof(uint32_t) + value_len);
                    if (len_ < increment) {
                        ParquetException::EofException();
                    }
                    RETURN_NOT_OK(builder->Append(data_ + sizeof(uint32_t), value_len));
                    data_ += increment;
                    len_ -= increment;
                    ++values_decoded;
                } else {
                    RETURN_NOT_OK(builder->AppendNull());
                }
                bit_reader.Next();
            }
            num_values_ -= values_decoded;
            *out_values_decoded = values_decoded;
            return Status::OK();
        }

        template<typename BuilderType>
        Status DecodeArrowNonNull(int num_values, BuilderType *builder, int *values_decoded) {
            num_values = std::min(num_values, num_values_);
            RETURN_NOT_OK(builder->Reserve(num_values));
            for (int i = 0; i < num_values; ++i) {
                uint32_t value_len = arrow::util::SafeLoadAs<uint32_t>(data_);
                int increment = static_cast<int>(sizeof(uint32_t) + value_len);
                if (len_ < increment) ParquetException::EofException();
                RETURN_NOT_OK(builder->Append(data_ + sizeof(uint32_t), value_len));
                data_ += increment;
                len_ -= increment;
            }
            num_values_ -= num_values;
            *values_decoded = num_values;
            return Status::OK();
        }
    };

    class PlainFLBADecoder : public PlainDecoder<FLBAType>, virtual public FLBADecoder {
    public:
        using Base = PlainDecoder<FLBAType>;
        using Base::PlainDecoder;
    };

// ----------------------------------------------------------------------
// Dictionary encoding and decoding

    template<typename Type>
    class DictDecoderImpl : public DecoderImpl, virtual public DictDecoder<Type> {
    public:
        typedef typename Type::c_type T;

        // Initializes the dictionary with values from 'dictionary'. The data in
        // dictionary is not guaranteed to persist in memory after this call so the
        // dictionary decoder needs to copy the data out if necessary.
        explicit DictDecoderImpl(const ColumnDescriptor *descr,
                                 MemoryPool *pool = arrow::default_memory_pool())
                : DecoderImpl(descr, Encoding::RLE_DICTIONARY),
                  dictionary_(AllocateBuffer(pool, 0)),
                  dictionary_length_(0),
                  byte_array_data_(AllocateBuffer(pool, 0)),
                  byte_array_offsets_(AllocateBuffer(pool, 0)),
                  indices_scratch_space_(AllocateBuffer(pool, 0)) {}

        // Perform type-specific initiatialization
        void SetDict(TypedDecoder<Type> *dictionary) override;

        void SetData(int num_values, const uint8_t *data, int len) override {
            num_values_ = num_values;
            if (len == 0) return;
            uint8_t bit_width = *data;
            ++data;
            --len;
            idx_decoder_ = arrow::util::RleDecoder(data, len, bit_width);
        }

        int Decode(T *buffer, int num_values) override {
            num_values = std::min(num_values, num_values_);
            int decoded_values = idx_decoder_.GetBatchWithDict(
                    reinterpret_cast<const T *>(dictionary_->data()), buffer, num_values);
            if (decoded_values != num_values) {
                ParquetException::EofException();
            }
            num_values_ -= num_values;
            return num_values;
        }

        int Skip(int num_values) override {
            num_values = std::min(num_values, num_values_);
            int skipped_values = idx_decoder_.Skip<T>(num_values);
            if (skipped_values != num_values) {
                ParquetException::EofException();
            }
            num_values_ -= num_values;
            return num_values;
        }

        int DecodeSpaced(T *buffer, int num_values, int null_count, const uint8_t *valid_bits,
                         int64_t valid_bits_offset) override {
            num_values = std::min(num_values, num_values_);
            if (num_values != idx_decoder_.GetBatchWithDictSpaced(
                    reinterpret_cast<const T *>(dictionary_->data()), buffer,
                    num_values, null_count, valid_bits, valid_bits_offset)) {
                ParquetException::EofException();
            }
            num_values_ -= num_values;
            return num_values;
        }

        int DecodeArrow(int num_values, int null_count, const uint8_t *valid_bits,
                        int64_t valid_bits_offset,
                        typename EncodingTraits<Type>::Accumulator *out) override;

        int DecodeArrow(int num_values, int null_count, const uint8_t *valid_bits,
                        int64_t valid_bits_offset,
                        typename EncodingTraits<Type>::DictAccumulator *out) override;

        void InsertDictionary(arrow::ArrayBuilder *builder) override;

        int DecodeIndicesSpaced(int num_values, int null_count, const uint8_t *valid_bits,
                                int64_t valid_bits_offset,
                                arrow::ArrayBuilder *builder) override {
            if (num_values > 0) {
                // TODO(wesm): Refactor to batch reads for improved memory use. It is not
                // trivial because the null_count is relative to the entire bitmap
                PARQUET_THROW_NOT_OK(indices_scratch_space_->TypedResize<int32_t>(
                        num_values, /*shrink_to_fit=*/false));
            }

            auto indices_buffer =
                    reinterpret_cast<int32_t *>(indices_scratch_space_->mutable_data());

            if (num_values != idx_decoder_.GetBatchSpaced(num_values, null_count, valid_bits,
                                                          valid_bits_offset, indices_buffer)) {
                ParquetException::EofException();
            }

            /// XXX(wesm): Cannot append "valid bits" directly to the builder
            std::vector<uint8_t> valid_bytes(num_values);
            arrow::internal::BitmapReader bit_reader(valid_bits, valid_bits_offset, num_values);
            for (int64_t i = 0; i < num_values; ++i) {
                valid_bytes[i] = static_cast<uint8_t>(bit_reader.IsSet());
                bit_reader.Next();
            }

            auto binary_builder = checked_cast<arrow::BinaryDictionary32Builder *>(builder);
            PARQUET_THROW_NOT_OK(
                    binary_builder->AppendIndices(indices_buffer, num_values, valid_bytes.data()));
            num_values_ -= num_values - null_count;
            return num_values - null_count;
        }

        int DecodeRaw(uint32_t *buffer, int num_values) override {
            num_values = std::min(num_values, num_values_);
            if (num_values != idx_decoder_.GetBatch(buffer, num_values)) {
                ParquetException::EofException();
            }
            num_values_ -= num_values;
            return num_values;
        }

        int DecodeIndices(int num_values, arrow::ArrayBuilder *builder) override {
            num_values = std::min(num_values, num_values_);
            if (num_values > 0) {
                // TODO(wesm): Refactor to batch reads for improved memory use. This is
                // relatively simple here because we don't have to do any bookkeeping of
                // nulls
                PARQUET_THROW_NOT_OK(indices_scratch_space_->TypedResize<int32_t>(
                        num_values, /*shrink_to_fit=*/false));
            }
            auto indices_buffer =
                    reinterpret_cast<int32_t *>(indices_scratch_space_->mutable_data());
            if (num_values != idx_decoder_.GetBatch(indices_buffer, num_values)) {
                ParquetException::EofException();
            }
            auto binary_builder = checked_cast<arrow::BinaryDictionary32Builder *>(builder);
            PARQUET_THROW_NOT_OK(binary_builder->AppendIndices(indices_buffer, num_values));
            num_values_ -= num_values;
            return num_values;
        }

    protected:
        inline void DecodeDict(TypedDecoder<Type> *dictionary) {
            dictionary_length_ = static_cast<int32_t>(dictionary->values_left());
            PARQUET_THROW_NOT_OK(dictionary_->Resize(dictionary_length_ * sizeof(T),
                    /*shrink_to_fit=*/false));
            dictionary->Decode(reinterpret_cast<T *>(dictionary_->mutable_data()),
                               dictionary_length_);
        }

        // Only one is set.
        std::shared_ptr<ResizableBuffer> dictionary_;

        int32_t dictionary_length_;

        // Data that contains the byte array data (byte_array_dictionary_ just has the
        // pointers).
        std::shared_ptr<ResizableBuffer> byte_array_data_;

        // Arrow-style byte offsets for each dictionary value. We maintain two
        // representations of the dictionary, one as ByteArray* for non-Arrow
        // consumers and this one for Arrow consumers. Since dictionaries are
        // generally pretty small to begin with this doesn't mean too much extra
        // memory use in most cases
        std::shared_ptr<ResizableBuffer> byte_array_offsets_;

        // Reusable buffer for decoding dictionary indices to be appended to a
        // BinaryDictionary32Builder
        std::shared_ptr<ResizableBuffer> indices_scratch_space_;

        arrow::util::RleDecoder idx_decoder_;
    };

    template<typename Type>
    void DictDecoderImpl<Type>::SetDict(TypedDecoder<Type> *dictionary) {
        DecodeDict(dictionary);
    }

    template<>
    void DictDecoderImpl<BooleanType>::SetDict(TypedDecoder<BooleanType> *dictionary) {
        ParquetException::NYI("Dictionary encoding is not implemented for boolean values");
    }

    template<>
    void DictDecoderImpl<ByteArrayType>::SetDict(TypedDecoder<ByteArrayType> *dictionary) {
        DecodeDict(dictionary);

        auto dict_values = reinterpret_cast<ByteArray *>(dictionary_->mutable_data());

        int total_size = 0;
        for (int i = 0; i < dictionary_length_; ++i) {
            total_size += dict_values[i].len;
        }
        if (total_size > 0) {
            PARQUET_THROW_NOT_OK(byte_array_data_->Resize(total_size,
                    /*shrink_to_fit=*/false));
            PARQUET_THROW_NOT_OK(
                    byte_array_offsets_->Resize((dictionary_length_ + 1) * sizeof(int32_t),
                            /*shrink_to_fit=*/false));
        }

        int32_t offset = 0;
        uint8_t *bytes_data = byte_array_data_->mutable_data();
        int32_t *bytes_offsets =
                reinterpret_cast<int32_t *>(byte_array_offsets_->mutable_data());
        for (int i = 0; i < dictionary_length_; ++i) {
            memcpy(bytes_data + offset, dict_values[i].ptr, dict_values[i].len);
            bytes_offsets[i] = offset;
            dict_values[i].ptr = bytes_data + offset;
            offset += dict_values[i].len;
        }
        bytes_offsets[dictionary_length_] = offset;
    }

    template<>
    inline void DictDecoderImpl<FLBAType>::SetDict(TypedDecoder<FLBAType> *dictionary) {
        DecodeDict(dictionary);

        auto dict_values = reinterpret_cast<FLBA *>(dictionary_->mutable_data());

        int fixed_len = descr_->type_length();
        int total_size = dictionary_length_ * fixed_len;

        PARQUET_THROW_NOT_OK(byte_array_data_->Resize(total_size,
                /*shrink_to_fit=*/false));
        uint8_t *bytes_data = byte_array_data_->mutable_data();
        for (int32_t i = 0, offset = 0; i < dictionary_length_; ++i, offset += fixed_len) {
            memcpy(bytes_data + offset, dict_values[i].ptr, fixed_len);
            dict_values[i].ptr = bytes_data + offset;
        }
    }

    template<>
    inline int DictDecoderImpl<Int96Type>::DecodeArrow(
            int num_values, int null_count, const uint8_t *valid_bits, int64_t valid_bits_offset,
            typename EncodingTraits<Int96Type>::Accumulator *builder) {
        ParquetException::NYI("DecodeArrow to Int96Type");
    }

    template<>
    inline int DictDecoderImpl<Int96Type>::DecodeArrow(
            int num_values, int null_count, const uint8_t *valid_bits, int64_t valid_bits_offset,
            typename EncodingTraits<Int96Type>::DictAccumulator *builder) {
        ParquetException::NYI("DecodeArrow to Int96Type");
    }

    template<>
    inline int DictDecoderImpl<ByteArrayType>::DecodeArrow(
            int num_values, int null_count, const uint8_t *valid_bits, int64_t valid_bits_offset,
            typename EncodingTraits<ByteArrayType>::Accumulator *builder) {
        ParquetException::NYI("DecodeArrow implemented elsewhere");
    }

    template<>
    inline int DictDecoderImpl<ByteArrayType>::DecodeArrow(
            int num_values, int null_count, const uint8_t *valid_bits, int64_t valid_bits_offset,
            typename EncodingTraits<ByteArrayType>::DictAccumulator *builder) {
        ParquetException::NYI("DecodeArrow implemented elsewhere");
    }

    template<typename DType>
    int DictDecoderImpl<DType>::DecodeArrow(
            int num_values, int null_count, const uint8_t *valid_bits, int64_t valid_bits_offset,
            typename EncodingTraits<DType>::DictAccumulator *builder) {
        PARQUET_THROW_NOT_OK(builder->Reserve(num_values));
        arrow::internal::BitmapReader bit_reader(valid_bits, valid_bits_offset, num_values);

        auto dict_values = reinterpret_cast<const typename DType::c_type *>(dictionary_->data());

        for (int i = 0; i < num_values; ++i) {
            bool is_valid = bit_reader.IsSet();
            bit_reader.Next();
            if (is_valid) {
                int32_t index;
                if (ARROW_PREDICT_FALSE(!idx_decoder_.Get(&index))) {
                    throw ParquetException("");
                }
                PARQUET_THROW_NOT_OK(builder->Append(dict_values[index]));
            } else {
                PARQUET_THROW_NOT_OK(builder->AppendNull());
            }
        }

        return num_values - null_count;
    }

    template<>
    int DictDecoderImpl<BooleanType>::DecodeArrow(
            int num_values, int null_count, const uint8_t *valid_bits, int64_t valid_bits_offset,
            typename EncodingTraits<BooleanType>::DictAccumulator *builder) {
        ParquetException::NYI("No dictionary encoding for BooleanType");
    }

    template<>
    inline int DictDecoderImpl<FLBAType>::DecodeArrow(
            int num_values, int null_count, const uint8_t *valid_bits, int64_t valid_bits_offset,
            typename EncodingTraits<FLBAType>::Accumulator *builder) {
        if (builder->byte_width() != descr_->type_length()) {
            throw ParquetException("Byte width mismatch: builder was " +
                                   std::to_string(builder->byte_width()) + " but decoder was " +
                                   std::to_string(descr_->type_length()));
        }

        PARQUET_THROW_NOT_OK(builder->Reserve(num_values));
        arrow::internal::BitmapReader bit_reader(valid_bits, valid_bits_offset, num_values);

        auto dict_values = reinterpret_cast<const FLBA *>(dictionary_->data());

        for (int i = 0; i < num_values; ++i) {
            bool is_valid = bit_reader.IsSet();
            bit_reader.Next();
            if (is_valid) {
                int32_t index;
                if (ARROW_PREDICT_FALSE(!idx_decoder_.Get(&index))) {
                    throw ParquetException("");
                }
                builder->UnsafeAppend(dict_values[index].ptr);
            } else {
                builder->UnsafeAppendNull();
            }
        }

        return num_values - null_count;
    }

    template<>
    int DictDecoderImpl<FLBAType>::DecodeArrow(
            int num_values, int null_count, const uint8_t *valid_bits, int64_t valid_bits_offset,
            typename EncodingTraits<FLBAType>::DictAccumulator *builder) {
        auto value_type =
                checked_cast<const arrow::DictionaryType &>(*builder->type()).value_type();
        auto byte_width =
                checked_cast<const arrow::FixedSizeBinaryType &>(*value_type).byte_width();
        if (byte_width != descr_->type_length()) {
            throw ParquetException("Byte width mismatch: builder was " +
                                   std::to_string(byte_width) + " but decoder was " +
                                   std::to_string(descr_->type_length()));
        }

        PARQUET_THROW_NOT_OK(builder->Reserve(num_values));
        arrow::internal::BitmapReader bit_reader(valid_bits, valid_bits_offset, num_values);

        auto dict_values = reinterpret_cast<const FLBA *>(dictionary_->data());

        for (int i = 0; i < num_values; ++i) {
            bool is_valid = bit_reader.IsSet();
            bit_reader.Next();
            if (is_valid) {
                int32_t index;
                if (ARROW_PREDICT_FALSE(!idx_decoder_.Get(&index))) {
                    throw ParquetException("");
                }
                PARQUET_THROW_NOT_OK(builder->Append(dict_values[index].ptr));
            } else {
                PARQUET_THROW_NOT_OK(builder->AppendNull());
            }
        }

        return num_values - null_count;
    }

    template<typename Type>
    int DictDecoderImpl<Type>::DecodeArrow(
            int num_values, int null_count, const uint8_t *valid_bits, int64_t valid_bits_offset,
            typename EncodingTraits<Type>::Accumulator *builder) {
        PARQUET_THROW_NOT_OK(builder->Reserve(num_values));
        arrow::internal::BitmapReader bit_reader(valid_bits, valid_bits_offset, num_values);

        using value_type = typename Type::c_type;
        auto dict_values = reinterpret_cast<const value_type *>(dictionary_->data());

        for (int i = 0; i < num_values; ++i) {
            bool is_valid = bit_reader.IsSet();
            bit_reader.Next();
            if (is_valid) {
                int32_t index;
                if (ARROW_PREDICT_FALSE(!idx_decoder_.Get(&index))) {
                    throw ParquetException("");
                }
                builder->UnsafeAppend(dict_values[index]);
            } else {
                builder->UnsafeAppendNull();
            }
        }

        return num_values - null_count;
    }

    template<typename Type>
    void DictDecoderImpl<Type>::InsertDictionary(arrow::ArrayBuilder *builder) {
        ParquetException::NYI("InsertDictionary only implemented for BYTE_ARRAY types");
    }

    template<>
    void DictDecoderImpl<ByteArrayType>::InsertDictionary(arrow::ArrayBuilder *builder) {
        auto binary_builder = checked_cast<arrow::BinaryDictionary32Builder *>(builder);

        // Make a BinaryArray referencing the internal dictionary data
        auto arr = std::make_shared<arrow::BinaryArray>(dictionary_length_, byte_array_offsets_,
                                                        byte_array_data_);
        PARQUET_THROW_NOT_OK(binary_builder->InsertMemoValues(*arr));
    }

    class DictByteArrayDecoderImpl : public DictDecoderImpl<ByteArrayType>,
                                     virtual public ByteArrayDecoder {
    public:
        using BASE = DictDecoderImpl<ByteArrayType>;
        using BASE::DictDecoderImpl;

        int DecodeArrow(int num_values, int null_count, const uint8_t *valid_bits,
                        int64_t valid_bits_offset,
                        arrow::BinaryDictionary32Builder *builder) override {
            int result = 0;
            PARQUET_THROW_NOT_OK(DecodeArrow(num_values, null_count, valid_bits,
                                             valid_bits_offset, builder, &result));
            return result;
        }

        int DecodeArrowNonNull(int num_values,
                               arrow::BinaryDictionary32Builder *builder) override {
            int result = 0;
            PARQUET_THROW_NOT_OK(DecodeArrowNonNull(num_values, builder, &result));
            return result;
        }

        int DecodeArrow(int num_values, int null_count, const uint8_t *valid_bits,
                        int64_t valid_bits_offset,
                        typename EncodingTraits<ByteArrayType>::Accumulator *out) override {
            int result = 0;
            PARQUET_THROW_NOT_OK(DecodeArrowDense(num_values, null_count, valid_bits,
                                                  valid_bits_offset, out, &result));
            return result;
        }

        int DecodeArrowNonNull(
                int num_values, typename EncodingTraits<ByteArrayType>::Accumulator *out) override {
            int result = 0;
            PARQUET_THROW_NOT_OK(DecodeArrowDenseNonNull(num_values, out, &result));
            return result;
        }

    private:
        Status DecodeArrowDense(int num_values, int null_count, const uint8_t *valid_bits,
                                int64_t valid_bits_offset,
                                typename EncodingTraits<ByteArrayType>::Accumulator *out,
                                int *out_num_values) {
            constexpr int32_t buffer_size = 1024;
            int32_t indices_buffer[buffer_size];

            ArrowBinaryHelper helper(out);

            arrow::internal::BitmapReader bit_reader(valid_bits, valid_bits_offset, num_values);

            auto dict_values = reinterpret_cast<const ByteArray *>(dictionary_->data());
            int values_decoded = 0;
            int num_appended = 0;
            while (num_appended < num_values) {
                bool is_valid = bit_reader.IsSet();
                bit_reader.Next();

                if (is_valid) {
                    int32_t batch_size =
                            std::min<int32_t>(buffer_size, num_values - num_appended - null_count);
                    int num_indices = idx_decoder_.GetBatch(indices_buffer, batch_size);

                    int i = 0;
                    while (true) {
                        // Consume all indices
                        if (is_valid) {
                            const auto &val = dict_values[indices_buffer[i]];
                            if (ARROW_PREDICT_FALSE(!helper.CanFit(val.len))) {
                                RETURN_NOT_OK(helper.PushChunk());
                            }
                            RETURN_NOT_OK(helper.Append(val.ptr, static_cast<int32_t>(val.len)));
                            ++i;
                            ++values_decoded;
                        } else {
                            RETURN_NOT_OK(helper.AppendNull());
                            --null_count;
                        }
                        ++num_appended;
                        if (i == num_indices) {
                            // Do not advance the bit_reader if we have fulfilled the decode
                            // request
                            break;
                        }
                        is_valid = bit_reader.IsSet();
                        bit_reader.Next();
                    }
                } else {
                    RETURN_NOT_OK(helper.AppendNull());
                    --null_count;
                    ++num_appended;
                }
            }
            *out_num_values = values_decoded;
            return Status::OK();
        }

        Status DecodeArrowDenseNonNull(int num_values,
                                       typename EncodingTraits<ByteArrayType>::Accumulator *out,
                                       int *out_num_values) {
            constexpr int32_t buffer_size = 2048;
            int32_t indices_buffer[buffer_size];
            int values_decoded = 0;

            ArrowBinaryHelper helper(out);
            auto dict_values = reinterpret_cast<const ByteArray *>(dictionary_->data());

            while (values_decoded < num_values) {
                int32_t batch_size = std::min<int32_t>(buffer_size, num_values - values_decoded);
                int num_indices = idx_decoder_.GetBatch(indices_buffer, batch_size);
                if (num_indices == 0) ParquetException::EofException();
                for (int i = 0; i < num_indices; ++i) {
                    const auto &val = dict_values[indices_buffer[i]];
                    if (ARROW_PREDICT_FALSE(!helper.CanFit(val.len))) {
                        RETURN_NOT_OK(helper.PushChunk());
                    }
                    RETURN_NOT_OK(helper.Append(val.ptr, static_cast<int32_t>(val.len)));
                }
                values_decoded += num_indices;
            }
            *out_num_values = values_decoded;
            return Status::OK();
        }

        template<typename BuilderType>
        Status DecodeArrow(int num_values, int null_count, const uint8_t *valid_bits,
                           int64_t valid_bits_offset, BuilderType *builder,
                           int *out_num_values) {
            constexpr int32_t buffer_size = 1024;
            int32_t indices_buffer[buffer_size];

            RETURN_NOT_OK(builder->Reserve(num_values));
            arrow::internal::BitmapReader bit_reader(valid_bits, valid_bits_offset, num_values);

            auto dict_values = reinterpret_cast<const ByteArray *>(dictionary_->data());

            int values_decoded = 0;
            int num_appended = 0;
            while (num_appended < num_values) {
                bool is_valid = bit_reader.IsSet();
                bit_reader.Next();

                if (is_valid) {
                    int32_t batch_size =
                            std::min<int32_t>(buffer_size, num_values - num_appended - null_count);
                    int num_indices = idx_decoder_.GetBatch(indices_buffer, batch_size);

                    int i = 0;
                    while (true) {
                        // Consume all indices
                        if (is_valid) {
                            const auto &val = dict_values[indices_buffer[i]];
                            RETURN_NOT_OK(builder->Append(val.ptr, val.len));
                            ++i;
                            ++values_decoded;
                        } else {
                            RETURN_NOT_OK(builder->AppendNull());
                            --null_count;
                        }
                        ++num_appended;
                        if (i == num_indices) {
                            // Do not advance the bit_reader if we have fulfilled the decode
                            // request
                            break;
                        }
                        is_valid = bit_reader.IsSet();
                        bit_reader.Next();
                    }
                } else {
                    RETURN_NOT_OK(builder->AppendNull());
                    --null_count;
                    ++num_appended;
                }
            }
            *out_num_values = values_decoded;
            return Status::OK();
        }

        template<typename BuilderType>
        Status DecodeArrowNonNull(int num_values, BuilderType *builder, int *out_num_values) {
            constexpr int32_t buffer_size = 2048;
            int32_t indices_buffer[buffer_size];
            int values_decoded = 0;
            RETURN_NOT_OK(builder->Reserve(num_values));

            auto dict_values = reinterpret_cast<const ByteArray *>(dictionary_->data());

            while (values_decoded < num_values) {
                int32_t batch_size = std::min<int32_t>(buffer_size, num_values - values_decoded);
                int num_indices = idx_decoder_.GetBatch(indices_buffer, batch_size);
                if (num_indices == 0) ParquetException::EofException();
                for (int i = 0; i < num_indices; ++i) {
                    const auto &val = dict_values[indices_buffer[i]];
                    RETURN_NOT_OK(builder->Append(val.ptr, val.len));
                }
                values_decoded += num_indices;
            }
            *out_num_values = values_decoded;
            return Status::OK();
        }
    };

// ----------------------------------------------------------------------
// DeltaBitPackDecoder

    template<typename DType>
    class DeltaBitPackDecoder : public DecoderImpl, virtual public TypedDecoder<DType> {
    public:
        typedef typename DType::c_type T;

        explicit DeltaBitPackDecoder(const ColumnDescriptor *descr,
                                     MemoryPool *pool = arrow::default_memory_pool())
                : DecoderImpl(descr, Encoding::DELTA_BINARY_PACKED), pool_(pool) {
            if (DType::type_num != Type::INT32 && DType::type_num != Type::INT64) {
                throw ParquetException("Delta bit pack encoding should only be for integer data.");
            }
        }

        void SetData(int num_values, const uint8_t *data, int len) override {
            this->num_values_ = num_values;
            decoder_ = arrow::BitUtil::BitReader(data, len);

            if (!decoder_.GetVlqInt(&block_size_)) ParquetException::EofException();
            if (!decoder_.GetVlqInt(&num_mini_blocks_)) ParquetException::EofException();

            values_per_mini_block_ = block_size_ / num_mini_blocks_;
            block_buffer_ = std::make_shared<std::vector<int32_t>>(block_size_ + 1);

            if (!decoder_.GetVlqInt(&total_values_)) {
                ParquetException::EofException();
            }
            values_read_ = 0;

            // The last value of previous block
            if (!decoder_.GetZigZagVlqInt(&last_value_)) ParquetException::EofException();

            delta_bit_widths_ = AllocateBuffer(pool_, num_mini_blocks_);

            block_buffer_->data()[0] = last_value_;
            num_buffered_ = 1;
            values_read_ += 1;
            ReadBlock();
        }

        int Decode(T *buffer, int max_values) override {
            return GetInternal(buffer, max_values);
        }

        int Skip(int max_values) override {
            return GetInternal((T *) nullptr, max_values);
        }

        int DecodeArrow(int num_values, int null_count, const uint8_t *valid_bits,
                        int64_t valid_bits_offset,
                        typename EncodingTraits<DType>::Accumulator *out) override {
            if (null_count != 0) {
                ParquetException::NYI("Delta bit pack DecodeArrow with null slots");
            }
            std::vector<T> values(num_values);
            GetInternal(values.data(), num_values);
            PARQUET_THROW_NOT_OK(out->AppendValues(values));
            return num_values;
        }

        int DecodeArrow(int num_values, int null_count, const uint8_t *valid_bits,
                        int64_t valid_bits_offset,
                        typename EncodingTraits<DType>::DictAccumulator *out) override {
            if (null_count != 0) {
                ParquetException::NYI("Delta bit pack DecodeArrow with null slots");
            }
            std::vector<T> values(num_values);
            GetInternal(values.data(), num_values);
            PARQUET_THROW_NOT_OK(out->Reserve(num_values));
            for (T value : values) {
                PARQUET_THROW_NOT_OK(out->Append(value));
            }
            return num_values;
        }

        inline uint32_t offset() { return decoder_.bytes_offset(); }

    private:
        void ReadBlock() {
            uint8_t *bit_width_data = delta_bit_widths_->mutable_data();
            if (!decoder_.GetZigZagVlqInt(&min_delta_)) ParquetException::EofException();

            for (int i = 0; i < num_mini_blocks_; ++i) {
                if (!decoder_.GetAligned<uint8_t>(1, bit_width_data + i)) {
                    ParquetException::EofException();
                }
            }

            int32_t *buffer = block_buffer_->data();
            int buffer_start = num_buffered_;
            for (int i = 0; i < num_mini_blocks_ && values_read_ < total_values_; ++i) {
                decoder_.GetBatch(bit_width_data[i], buffer + num_buffered_, values_per_mini_block_);
                num_buffered_ += values_per_mini_block_;
                values_read_ += values_per_mini_block_;
            }

            // Speed up Delta computation using Sboost
            for (uint32_t i = buffer_start; i < num_buffered_; i += 8) {
                int32_t *position = buffer + i;
                __m256i loaded = _mm256_loadu_si256((const __m256i *) position);
                loaded = _mm256_add_epi32(loaded, _mm256_set1_epi32(min_delta_));
                __m256i result = ::sboost::cumsum32(loaded);
                result = _mm256_add_epi32(result, _mm256_set1_epi32(last_value_));
                _mm256_storeu_si256((__m256i *) position, result);
                last_value_ = *(position + 7);
            }
//    last_value_ = buffer[num_buffered_-1];
            num_buffer_read_ = 0;
        }

        template<typename T>
        int GetInternal(T *buffer, int max_values) {
            max_values = std::min(max_values, this->num_values_);
            int *bufferdata = block_buffer_->data();

            int write_pos = 0;
            int remain = max_values;
            int read_in_block = std::min(max_values, static_cast<int>(num_buffered_ - num_buffer_read_));
            if (buffer != nullptr)
                memcpy(buffer, bufferdata + num_buffer_read_, sizeof(T) * read_in_block);
            remain -= read_in_block;
            write_pos += read_in_block;
            num_buffer_read_ += read_in_block;

            while (remain > 0) {
                num_buffered_ = 0;
                ReadBlock();
                read_in_block = std::min(remain, static_cast<int>(num_buffered_ - num_buffer_read_));
                if (nullptr != buffer)
                    memcpy(buffer + write_pos, bufferdata + num_buffer_read_, sizeof(T) * read_in_block);
                remain -= read_in_block;
                write_pos += read_in_block;
                num_buffer_read_ += read_in_block;
            }

            this->num_values_ -= max_values;
            return max_values;
        }

        MemoryPool *pool_;
        arrow::BitUtil::BitReader decoder_;
        int32_t total_values_;
        int32_t values_read_;

        int32_t block_size_;
        int32_t num_mini_blocks_;
        uint64_t values_per_mini_block_;

        uint64_t num_buffered_;
        uint64_t num_buffer_read_;

        int32_t min_delta_;
        std::shared_ptr<ResizableBuffer> delta_bit_widths_;
        std::shared_ptr<std::vector<int32_t>> block_buffer_;

        int32_t last_value_;

    };

// ----------------------------------------------------------------------
// DELTA_LENGTH_BYTE_ARRAY

    class DeltaLengthByteArrayDecoder : public DecoderImpl,
                                        virtual public TypedDecoder<ByteArrayType> {
    public:
        explicit DeltaLengthByteArrayDecoder(const ColumnDescriptor *descr,
                                             MemoryPool *pool = arrow::default_memory_pool())
                : DecoderImpl(descr, Encoding::DELTA_LENGTH_BYTE_ARRAY),
                  len_decoder_(nullptr, pool),
                  pool_(pool),
                  len_data_(0, ::arrow::stl::allocator<int>(pool_)),
                  len_data_pointer_(0) {}

        void SetData(int num_values, const uint8_t *data, int len) override {
            num_values_ = num_values;
            if (len == 0) return;
            this->len_decoder_.SetData(num_values, data, len);
            if (len_data_.size() < (uint32_t) num_values) {
                len_data_.resize(num_values);
            }
            len_decoder_.Decode(len_data_.data(), num_values);
            len_data_pointer_ = 0;

            uint32_t offset = len_decoder_.offset();
            data_ = data + offset;
            this->len_ = len - offset;
        }

        int Decode(ByteArray *buffer, int max_values) override {
            max_values = std::min(max_values, num_values_);
            for (int i = 0; i < max_values; ++i) {
                auto len = len_data_[len_data_pointer_++];
                buffer[i].len = len;
                buffer[i].ptr = data_;
                this->data_ += len;
                this->len_ -= len;
            }
            this->num_values_ -= max_values;
            return max_values;
        }

        int Skip(int max_values) override {
            max_values = std::min(max_values, num_values_);
            for (int i = 0; i < max_values; ++i) {
                auto len = len_data_[len_data_pointer_++];
                this->data_ += len;
                this->len_ -= len;
            }
            this->num_values_ -= max_values;
            return max_values;
        }

        int DecodeArrow(int num_values, int null_count, const uint8_t *valid_bits,
                        int64_t valid_bits_offset,
                        typename EncodingTraits<ByteArrayType>::Accumulator *out) override {
            ParquetException::NYI("DecodeArrow for DeltaLengthByteArrayDecoder");
        }

        int DecodeArrow(int num_values, int null_count, const uint8_t *valid_bits,
                        int64_t valid_bits_offset,
                        typename EncodingTraits<ByteArrayType>::DictAccumulator *out) override {
            ParquetException::NYI("DecodeArrow for DeltaLengthByteArrayDecoder");
        }

    private:
        DeltaBitPackDecoder<Int32Type> len_decoder_;
        ::arrow::MemoryPool *pool_;

        ArrowPoolVector<int32_t> len_data_;
        uint32_t len_data_pointer_;
    };

// ----------------------------------------------------------------------
// DELTA_BYTE_ARRAY

#define DBA_BUFFER_SIZE 1048576

    class DeltaByteArrayDecoder : public DecoderImpl,
                                  virtual public TypedDecoder<ByteArrayType> {
    public:
        explicit DeltaByteArrayDecoder(const ColumnDescriptor *descr,
                                       MemoryPool *pool = arrow::default_memory_pool())
                : DecoderImpl(descr, Encoding::DELTA_BYTE_ARRAY),
                  prefix_len_decoder_(nullptr, pool),
                  suffix_decoder_(nullptr, pool),
                  last_value_{0, nullptr},
                  prefix_len_data_(0, 0, ::arrow::stl::allocator<int>(pool)),
                  prefix_len_data_pointer_(0),
                  pool_(pool) {
            AllocateBuffer();
        }

        DeltaByteArrayDecoder(DeltaByteArrayDecoder &) = delete;

        DeltaByteArrayDecoder(DeltaByteArrayDecoder &&) = delete;

        DeltaByteArrayDecoder &operator=(DeltaByteArrayDecoder &) = delete;

        DeltaByteArrayDecoder &operator=(DeltaByteArrayDecoder &&) = delete;

        virtual ~DeltaByteArrayDecoder() {
            for (auto b: buffers_) {
                pool_->Free(b, DBA_BUFFER_SIZE);
            }
        }

        void AllocateBuffer() {
            auto assigned = pool_->Allocate(DBA_BUFFER_SIZE, &current_buffer_);
            buffers_.push_back(current_buffer_);
            buffer_pointer_ = 0;
        }

        virtual void SetData(int num_values, const uint8_t *data, int len) override {
            if (prefix_len_data_.size() < (uint32_t) num_values) {
                prefix_len_data_.resize(num_values);
            }
            prefix_len_data_pointer_ = 0;

            num_values_ = num_values;
            if (len == 0) return;
            prefix_len_decoder_.SetData(num_values, data, len);
            prefix_len_decoder_.Decode(prefix_len_data_.data(), num_values);

            uint32_t offset = prefix_len_decoder_.offset();
            suffix_decoder_.SetData(num_values, data + offset, len - offset);
        }

        // TODO: this doesn't work and requires memory management. We need to allocate
        // new strings to store the results.
        virtual int Decode(ByteArray *buffer, int max_values) override {
            ByteArray *local_last_value = &last_value_;
            max_values = std::min(max_values, this->num_values_);
            suffix_decoder_.Decode(buffer, max_values);
            for (int i = 0; i < max_values; ++i) {
                int prefix_len = prefix_len_data_[prefix_len_data_pointer_++];
//                ByteArray suffix = {0, nullptr};
//                suffix_decoder_.Decode(&suffix, 1);
                ByteArray suffix = buffer[i];
                buffer[i].len += prefix_len;

                if (DBA_BUFFER_SIZE - buffer_pointer_ < buffer[i].len) {
                    AllocateBuffer();
                }
                buffer[i].ptr = current_buffer_ + buffer_pointer_;
                if (prefix_len > 0)
                    memcpy(current_buffer_ + buffer_pointer_, local_last_value->ptr, prefix_len);
                memcpy(current_buffer_ + buffer_pointer_ + prefix_len, suffix.ptr, suffix.len);
                buffer_pointer_ += buffer[i].len;
                local_last_value = buffer + i;
            }
            last_value_ = *local_last_value;
            this->num_values_ -= max_values;
            return max_values;
        }

        // Skipped data will not be kept in buffer
        virtual int Skip(int max_values) override {
            max_values = std::min(max_values, this->num_values_);
            ByteArray value_buffer;
            ByteArray suffix = {0, nullptr};
            ByteArray *local_last_value = &last_value_;
            for (int i = 0; i < max_values; ++i) {
                int prefix_len = prefix_len_data_[prefix_len_data_pointer_++];
                suffix_decoder_.Decode(&suffix, 1);
                value_buffer.len = prefix_len + suffix.len;

                if (DBA_BUFFER_SIZE - buffer_pointer_ < value_buffer.len) {
                    AllocateBuffer();
                }
                if (prefix_len > 0)
                    memcpy(current_buffer_ + buffer_pointer_, local_last_value->ptr, prefix_len);
                memcpy(current_buffer_ + buffer_pointer_ + prefix_len, suffix.ptr, suffix.len);
                value_buffer.ptr = current_buffer_ + buffer_pointer_;
                // Do not increase buffer_pointer
                local_last_value = &value_buffer;
            }
            // Keep the last_value to buffer
            last_value_ = *local_last_value;
            buffer_pointer_ += last_value_.len;

            this->num_values_ -= max_values;
            return max_values;
        }


        int DecodeArrow(int num_values, int null_count, const uint8_t *valid_bits,
                        int64_t valid_bits_offset,
                        typename EncodingTraits<ByteArrayType>::Accumulator *out) override {
            ParquetException::NYI("DecodeArrow for DeltaByteArrayDecoder");
        }

        int DecodeArrow(int num_values, int null_count, const uint8_t *valid_bits,
                        int64_t valid_bits_offset,
                        typename EncodingTraits<ByteArrayType>::DictAccumulator *out) override {
            ParquetException::NYI("DecodeArrow for DeltaByteArrayDecoder");
        }

    private:
        DeltaBitPackDecoder<Int32Type> prefix_len_decoder_;
        DeltaLengthByteArrayDecoder suffix_decoder_;
        ByteArray last_value_;

        ArrowPoolVector<int32_t> prefix_len_data_;
        uint32_t prefix_len_data_pointer_;

        MemoryPool *pool_;
        vector<uint8_t *> buffers_;
        uint8_t *current_buffer_;
        uint32_t buffer_pointer_;
    };

// ----------------------------------------------------------------------

    std::unique_ptr<Decoder> MakeDecoder(Type::type type_num, Encoding::type encoding,
                                         const ColumnDescriptor *descr) {
        if (encoding == Encoding::PLAIN) {
            switch (type_num) {
                case Type::BOOLEAN:
                    return std::unique_ptr<Decoder>(new PlainBooleanDecoder(descr));
                case Type::INT32:
                    return std::unique_ptr<Decoder>(new PlainDecoder<Int32Type>(descr));
                case Type::INT64:
                    return std::unique_ptr<Decoder>(new PlainDecoder<Int64Type>(descr));
                case Type::INT96:
                    return std::unique_ptr<Decoder>(new PlainDecoder<Int96Type>(descr));
                case Type::FLOAT:
                    return std::unique_ptr<Decoder>(new PlainDecoder<FloatType>(descr));
                case Type::DOUBLE:
                    return std::unique_ptr<Decoder>(new PlainDecoder<DoubleType>(descr));
                case Type::BYTE_ARRAY:
                    return std::unique_ptr<Decoder>(new PlainByteArrayDecoder(descr));
                case Type::FIXED_LEN_BYTE_ARRAY:
                    return std::unique_ptr<Decoder>(new PlainFLBADecoder(descr));
                default:
                    break;
            }
        } else if (encoding == Encoding::DELTA_BINARY_PACKED) {
            return std::unique_ptr<Decoder>(new DeltaBitPackDecoder<Int32Type>(descr));
        } else if (encoding == Encoding::DELTA_BYTE_ARRAY) {
            return std::unique_ptr<Decoder>(new DeltaByteArrayDecoder(descr));
        } else if (encoding == Encoding::DELTA_LENGTH_BYTE_ARRAY) {
            return std::unique_ptr<Decoder>(new DeltaLengthByteArrayDecoder(descr));
        } else {
            ParquetException::NYI("Selected encoding is not supported");
        }
                DCHECK(false) << "Should not be able to reach this code";
        return nullptr;
    }

    namespace detail {
        std::unique_ptr<Decoder> MakeDictDecoder(Type::type type_num,
                                                 const ColumnDescriptor *descr,
                                                 MemoryPool *pool) {
            switch (type_num) {
                case Type::BOOLEAN:
                    ParquetException::NYI("Dictionary encoding not implemented for boolean type");
                case Type::INT32:
                    return std::unique_ptr<Decoder>(new DictDecoderImpl<Int32Type>(descr, pool));
                case Type::INT64:
                    return std::unique_ptr<Decoder>(new DictDecoderImpl<Int64Type>(descr, pool));
                case Type::INT96:
                    return std::unique_ptr<Decoder>(new DictDecoderImpl<Int96Type>(descr, pool));
                case Type::FLOAT:
                    return std::unique_ptr<Decoder>(new DictDecoderImpl<FloatType>(descr, pool));
                case Type::DOUBLE:
                    return std::unique_ptr<Decoder>(new DictDecoderImpl<DoubleType>(descr, pool));
                case Type::BYTE_ARRAY:
                    return std::unique_ptr<Decoder>(new DictByteArrayDecoderImpl(descr, pool));
                case Type::FIXED_LEN_BYTE_ARRAY:
                    return std::unique_ptr<Decoder>(new DictDecoderImpl<FLBAType>(descr, pool));
                default:
                    break;
            }
                    DCHECK(false) << "Should not be able to reach this code";
            return nullptr;
        }

    }  // namespace detail
}  // namespace parquet
