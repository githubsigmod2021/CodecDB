//
// Created by harper on 2/9/20.
//

#include <iostream>
#include <exception>
#include <arrow/util/bit_stream_utils.h>
#include <parquet/encoding.h>
#include <parquet/column_reader.h>
#include "validate.h"
#include "data_model.h"
#include "memorypool.h"

using namespace std;

namespace lqf {

    ostream &operator<<(ostream &os, MemDataRow &dt) {
        uint64_t *raw = dt.raw();
        auto offsets = dt.offset();
        auto limit = dt.offset().size() - 1;
        for (uint32_t i = 0; i < limit; ++i) {
            auto size = offsets[i + 1] - offsets[i];
            if (size == 1) {
                os.write((char *) (raw + offsets[i]), 8);
            } else {
                auto value = dt[i].asByteArray();
                os << string((const char *) value.ptr, value.len);
            }
        }
        return os;
    }

    ostream &operator<<(ostream &os, DataField &dt) {
        if (dt.size_ == 1) {
            os.write((char *) dt.data(), 8);
        } else {
            auto value = dt.asByteArray();
            os.write((const char *) value.ptr, value.len);
        }
        return os;
    }

    mt19937 Block::rand_ = mt19937(time(NULL));

    const array<vector<uint32_t>, 11> OFFSETS = {
            vector<uint32_t>({0}),
            {0, 1},
            {0, 1, 2},
            {0, 1, 2, 3},
            {0, 1, 2, 3, 4},
            {0, 1, 2, 3, 4, 5},
            {0, 1, 2, 3, 4, 5, 6},
            {0, 1, 2, 3, 4, 5, 6, 7},
            {0, 1, 2, 3, 4, 5, 6, 7, 8},
            {0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
            {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
    };

    const array<vector<uint32_t>, 11> SIZES = {
            vector<uint32_t>({}),
            vector<uint32_t>({1}),
            vector<uint32_t>({1, 1}),
            vector<uint32_t>({1, 1, 1}),
            vector<uint32_t>({1, 1, 1, 1}),
            vector<uint32_t>({1, 1, 1, 1, 1}),
            vector<uint32_t>({1, 1, 1, 1, 1, 1}),
            vector<uint32_t>({1, 1, 1, 1, 1, 1, 1}),
            vector<uint32_t>({1, 1, 1, 1, 1, 1, 1, 1}),
            vector<uint32_t>({1, 1, 1, 1, 1, 1, 1, 1, 1}),
            vector<uint32_t>({1, 1, 1, 1, 1, 1, 1, 1, 1, 1})
    };

    const vector<uint32_t> &colOffset(uint32_t num_fields) {
        return OFFSETS[num_fields];
    }

    const vector<uint32_t> &colSize(uint32_t num_fields) {
        return SIZES[num_fields];
    }

    vector<uint32_t> offset2size(const vector<uint32_t> &offset) {
        vector<uint32_t> size;
        for (auto i = 1u; i < offset.size(); ++i) {
            size.push_back(offset[i] - offset[i - 1]);
        }
        return size;
    }

    vector<uint32_t> size2offset(const vector<uint32_t> &size) {
        vector<uint32_t> offset;
        auto last = 0u;
        offset.push_back(last);
        for (auto &i:size) {
            last += i;
            offset.push_back(last);
        }
        return offset;
    }

    class MemRowRef : public DataRow {
    private:
        uint64_t *start_;
        const vector<uint32_t> &col_offset_;
        DataField view_;
    public:
        MemRowRef(uint64_t *start, const vector<uint32_t> &offset)
                : start_(start), col_offset_(offset) {}

        virtual ~MemRowRef() = default;

        unique_ptr<DataRow> snapshot() override {
            return nullptr;
        }

        DataField &operator[](uint64_t i) override {
            view_ = start_ + col_offset_[i];
            view_.size_ = col_offset_[i + 1] - col_offset_[i];
            return view_;
        }

        uint64_t *raw() override {
            return start_;
        }

        uint32_t size() override {
            return col_offset_.back();
        }

        uint32_t num_fields() override {
            return col_offset_.size() - 1;
        }

        DataRow &operator=(DataRow &row) override {
            if (row.raw()) {
                memcpy(static_cast<void *>(start_), static_cast<void *>(row.raw()),
                       sizeof(uint64_t) * col_offset_.back());
            } else {
                auto offset_size = col_offset_.size();
                for (uint32_t i = 0; i < offset_size - 1; ++i) {
                    (*this)[i] = row[i];
                }
            }
            return *this;
        }
    };

    MemDataRow MemDataRow::EMPTY = MemDataRow(0);

    MemDataRow::MemDataRow(uint8_t num_fields)
            : MemDataRow(OFFSETS[num_fields]) {}

    MemDataRow::MemDataRow(const vector<uint32_t> &offset)
            : data_(offset.back(), 0x0), offset_(offset) {}

    unique_ptr<DataRow> MemDataRow::snapshot() {
        return unique_ptr<DataRow>(new MemRowRef(data_.data(), offset_));
    }

    DataField &MemDataRow::operator[](uint64_t i) {
        view_ = data_.data() + offset_[i];
        assert(i + 1 < offset_.size());
        view_.size_ = offset_[i + 1] - offset_[i];
        return view_;
    }

    DataRow &MemDataRow::operator=(DataRow &row) {
        if (row.raw()) {
            memcpy(static_cast<void *>(data_.data()), static_cast<void *>(row.raw()),
                   sizeof(uint64_t) * data_.size());
        } else {
            auto offset_size = offset_.size();
            for (uint32_t i = 0; i < offset_size - 1; ++i) {
                (*this)[i] = row[i];
            }
        }
        return *this;
    }

    MemDataRow &MemDataRow::operator=(MemDataRow &row) {
        memcpy(static_cast<void *>(data_.data()), static_cast<void *>(row.data_.data()),
               sizeof(uint64_t) * data_.size());
        return *this;
    }

    uint64_t *MemDataRow::raw() {
        return data_.data();
    }

    MemBlock::MemBlock(uint32_t size, const vector<uint32_t> &col_offset)
            : size_(size), row_size_(col_offset.back()), col_offset_(col_offset) {
        content_ = vector<uint64_t>(size * row_size_);
    }

    MemBlock::MemBlock(uint32_t size, uint32_t row_size) : MemBlock(size, OFFSETS[row_size]) {}

    uint64_t MemBlock::size() {
        return size_;
    }

    void MemBlock::resize(uint32_t newsize) {
        content_.resize(newsize * row_size_);
        size_ = newsize;
    }

    vector<uint64_t> &MemBlock::content() {
        return content_;
    }

    class MemDataRowIterator;

    class MemDataRowView : public DataRow {
    private:
        vector<uint64_t> &data_;
        uint64_t index_;
        uint32_t row_size_;
        const vector<uint32_t> &col_offset_;
        DataField view_;
        friend MemDataRowIterator;
    public:
        MemDataRowView(vector<uint64_t> &data, uint32_t row_size, const vector<uint32_t> &col_offset)
                : data_(data), index_(-1), row_size_(row_size), col_offset_(col_offset) {}

        virtual ~MemDataRowView() {}

        void moveto(uint64_t index) { index_ = index; }

        void next() { ++index_; }

        unique_ptr<DataRow> snapshot() override {
            MemRowRef *snapshot = new MemRowRef(data_.data() + index_ * row_size_, col_offset_);
            return unique_ptr<DataRow>(snapshot);
        }

        DataField &operator[](uint64_t i) override {
            assert(index_ < data_.size() / row_size_);
            assert(col_offset_[i] < row_size_);
            view_ = data_.data() + index_ * row_size_ + col_offset_[i];
            view_.size_ = col_offset_[i + 1] - col_offset_[i];
            return view_;
        }

        uint64_t *raw() override {
            return data_.data() + index_ * row_size_;
        }

        uint32_t size() override {
            return row_size_;
        }

        uint32_t num_fields() override {
            return col_offset_.size() - 1;
        }

        DataRow &operator=(DataRow &row) override {
            if (row.raw()) {
                memcpy(static_cast<void *>(data_.data() + index_ * row_size_), static_cast<void *>(row.raw()),
                       sizeof(uint64_t) * row_size_);
            } else {
                auto offset_size = col_offset_.size();
                for (uint32_t i = 0; i < offset_size - 1; ++i) {
                    (*this)[i] = row[i];
                }
            }
            return *this;
        }
    };

    class MemDataRowIterator : public DataRowIterator {
    private:
        MemDataRowView reference_;
    public:
        MemDataRowIterator(vector<uint64_t> &data, uint32_t row_size, const vector<uint32_t> &col_offset)
                : reference_(data, row_size, col_offset) {}

        DataRow &operator[](uint64_t idx) override {
            reference_.moveto(idx);
            return reference_;
        }

        DataRow &next() override {
            reference_.next();
            return reference_;
        }

        uint64_t pos() override {
            return reference_.index_;
        }
    };

    class MemColumnIterator : public ColumnIterator {
    private:
        vector<uint64_t> &data_;
        uint32_t row_size_;
        uint32_t col_offset_;
        uint64_t row_index_;
        DataField view_;
    public:
        MemColumnIterator(vector<uint64_t> &data, uint32_t row_size, uint32_t col_offset, uint32_t col_size)
                : data_(data), row_size_(row_size), col_offset_(col_offset), row_index_(-1) {
            view_.size_ = col_size;
        }

        DataField &operator[](uint64_t idx) override {
            assert(idx < data_.size() / row_size_);
            row_index_ = idx;
            view_ = data_.data() + idx * row_size_ + col_offset_;
            return view_;
        }

        DataField &next() override {
            view_ = data_.data() + (++row_index_) * row_size_ + col_offset_;
            return view_;
        }

        uint64_t pos() override {
            return row_index_;
        }
    };

    unique_ptr<DataRowIterator> MemBlock::rows() {
        return unique_ptr<DataRowIterator>(new MemDataRowIterator(content_, row_size_, col_offset_));
    }

    unique_ptr<ColumnIterator> MemBlock::col(uint32_t col_index) {
        return unique_ptr<ColumnIterator>(new MemColumnIterator(content_, row_size_, col_offset_[col_index],
                                                                col_offset_[col_index + 1] - col_offset_[col_index]));
    }

    shared_ptr<Block> MemBlock::mask(shared_ptr<Bitmap> mask) {
        return make_shared<MaskedBlock>(shared_from_this(), mask);
    }

    MemvBlock::MemvBlock(uint32_t size, const vector<uint32_t> &col_size) : size_(size), col_size_(col_size) {
        uint8_t num_fields = col_size.size();
        for (uint8_t i = 0; i < num_fields; ++i) {
            content_.push_back(unique_ptr<vector<uint64_t>>(new vector<uint64_t>(size * col_size_[i])));
        }
    }

    MemvBlock::MemvBlock(uint32_t size, uint32_t num_fields) : MemvBlock(size, SIZES[num_fields]) {}

    uint64_t MemvBlock::size() {
        return size_;
    }

    void MemvBlock::resize(uint32_t newsize) {
        size_ = newsize;
        uint8_t num_fields = col_size_.size();
        for (uint8_t i = 0; i < num_fields; ++i) {
            content_[i]->resize(size_ * col_size_[i]);
        }
    }

    class MemvColumnIterator : public ColumnIterator {
    private:
        vector<uint64_t> &data_;
        uint64_t row_index_;
        DataField view_;
    public:
        MemvColumnIterator(vector<uint64_t> &data, uint32_t col_size)
                : data_(data), row_index_(-1) {
            assert(col_size <= 2);
            view_.size_ = col_size;
        }

        DataField &operator[](uint64_t idx) override {
            assert(idx < data_.size() / view_.size_);
            row_index_ = idx;
            view_ = data_.data() + idx * view_.size_;
            return view_;
        }

        DataField &next() override {
            ++row_index_;
            view_ = data_.data() + row_index_ * view_.size_;
            return view_;
        }

        uint64_t pos() override {
            return row_index_;
        }
    };

    class MemvDataRowIterator;

    class MemvDataRowView : public DataRow {
    private:
        vector<unique_ptr<vector<uint64_t>>> *cols_;
        const vector<uint32_t> &col_size_;
        uint64_t index_;
        DataField view_;

        friend MemvDataRowIterator;
    public:
        MemvDataRowView(vector<unique_ptr<vector<uint64_t>>> *cols, const vector<uint32_t> &col_sizes)
                : cols_(cols), col_size_(col_sizes), index_(-1) {
        }

        virtual ~MemvDataRowView() {}

        void moveto(uint64_t index) {
            index_ = index;
        }

        void next() {
            ++index_;
        }

        DataField &operator[](uint64_t i) override {
            view_.size_ = col_size_[i];
            view_ = (*cols_)[i]->data() + view_.size_ * index_;
            return view_;
        }

        uint32_t num_fields() override {
            return col_size_.size();
        }

        DataRow &operator=(DataRow &row) override {
            uint32_t num_cols = cols_->size();
            for (uint32_t i = 0; i < num_cols; ++i) {
                (*this)[i] = row[i];
            }
            return *this;
        }

        unique_ptr<DataRow> snapshot() override {
            MemvDataRowView *view = new MemvDataRowView(cols_, col_size_);
            view->index_ = index_;
            return unique_ptr<DataRow>(view);
        }
    };

    class MemvDataRowIterator : public DataRowIterator {
    private:
        MemvDataRowView reference_;
    public:
        MemvDataRowIterator(vector<unique_ptr<vector<uint64_t>>> *cols, const vector<uint32_t> &col_sizes)
                : reference_(cols, col_sizes) {}

        DataRow &operator[](uint64_t idx) override {
            reference_.moveto(idx);
            return reference_;
        }

        DataRow &next() override {
            reference_.next();
            return reference_;
        }

        uint64_t pos() override {
            return reference_.index_;
        }
    };

    unique_ptr<DataRowIterator> MemvBlock::rows() {
        return unique_ptr<DataRowIterator>(new MemvDataRowIterator(&content_, col_size_));
    }

    unique_ptr<ColumnIterator> MemvBlock::col(uint32_t col_index) {
        return unique_ptr<ColumnIterator>(new MemvColumnIterator(*content_[col_index], col_size_[col_index]));
    }

    shared_ptr<Block> MemvBlock::mask(shared_ptr<Bitmap> mask) {
        // Does not support
        return make_shared<MaskedBlock>(shared_from_this(), mask);
    }

    void MemvBlock::merge(MemvBlock &another, const vector<pair<uint8_t, uint8_t>> &merge_inst) {
        this->size_ = std::max(size_, another.size_);
        for (auto &inst: merge_inst) {
            content_[inst.second] = move(another.content_[inst.first]);
        }
        // The old memblock is discarded
        another.content_.clear();
    }

    MemDataRowPointer::MemDataRowPointer(const vector<uint32_t> &col_offset)
            : offset_(col_offset) {}

    DataField &MemDataRowPointer::operator[](uint64_t i) {
        view_ = pointer_ + offset_[i];
        view_.size_ = offset_[i + 1] - offset_[i];
        return view_;
    }

    uint64_t *MemDataRowPointer::raw() {
        return pointer_;
    }

    unique_ptr<DataRow> MemDataRowPointer::snapshot() {
        return unique_ptr<DataRow>(new MemRowRef(pointer_, offset_));
    }

    DataRow &MemDataRowPointer::operator=(DataRow &row) {
        if (row.raw()) {
            memcpy(static_cast<void *>(pointer_), static_cast<void *>(row.raw()),
                   sizeof(uint64_t) * offset_.back());
        } else {
            auto offset_size = offset_.size();
            for (uint32_t i = 0; i < offset_size - 1; ++i) {
                (*this)[i] = row[i];
            }
        }
        return *this;
    }

    MemFlexBlock::MemFlexBlock(const vector<uint32_t> &col_offset)
            : MemFlexBlock(col_offset, FLEX_SLAB_SIZE_) {}

    MemFlexBlock::MemFlexBlock(const vector<uint32_t> &col_offset, uint32_t slab_size)
            : slab_size_(slab_size), size_(0), col_offset_(col_offset), pointer_(0), accessor_(col_offset_) {
        memory_.push_back(make_shared<vector<uint64_t>>(slab_size_));
        row_size_ = col_offset.back();
        stripe_size_ = slab_size_ / row_size_;
    }

    uint64_t MemFlexBlock::size() {
        return size_;
    }

    DataRow &MemFlexBlock::push_back() {
        auto current = pointer_;
        pointer_ += row_size_;
        if (pointer_ > slab_size_) {
            memory_.push_back(make_shared<vector<uint64_t>>(slab_size_));
            pointer_ = row_size_;
            current = 0;
        }
        size_++;
        accessor_.raw(memory_.back()->data() + current);
        return accessor_;
    }

    DataRow &MemFlexBlock::operator[](uint32_t index) {
        auto stripe_index = index / stripe_size_;
        auto offset = index % stripe_size_;
        accessor_.raw(memory_[stripe_index]->data() + offset * row_size_);
        return accessor_;
    }

    void MemFlexBlock::assign(vector<shared_ptr<vector<uint64_t>>> &content, uint32_t slab_size, uint32_t size) {
        size_ = size;
        slab_size_ = slab_size;
        stripe_size_ = slab_size_ / row_size_;
        memory_ = move(content);
    }

    shared_ptr<Block> MemFlexBlock::mask(shared_ptr<Bitmap> mask) {
        return make_shared<MaskedBlock>(shared_from_this(), mask);
    }

    class FlexColumnIterator : public ColumnIterator {
    private:
        vector<shared_ptr<vector<uint64_t>>> &data_;
        uint32_t stripe_size_;
        uint32_t row_size_;
        uint32_t col_offset_;
        uint64_t row_index_;
        uint64_t stripe_index_;
        uint64_t stripe_offset_;
        DataField view_;
    public:
        FlexColumnIterator(vector<shared_ptr<vector<uint64_t>>> &data, uint32_t stripe_size,
                           uint32_t row_size, uint32_t col_offset, uint32_t col_size)
                : data_(data), stripe_size_(stripe_size), row_size_(row_size), col_offset_(col_offset),
                  row_index_(-1), stripe_index_(0), stripe_offset_(-1) {
            view_.size_ = col_size;
        }

        DataField &operator[](uint64_t idx) override {
            row_index_ = idx;
            stripe_index_ = idx / stripe_size_;
            stripe_offset_ = idx % stripe_size_;
            view_ = data_[stripe_index_]->data() + stripe_offset_ * row_size_ + col_offset_;
            return view_;
        }

        DataField &next() override {
            ++row_index_;
            ++stripe_offset_;
            if (stripe_offset_ >= stripe_size_) {
                stripe_index_ += 1;
                stripe_offset_ = 0;
            }
            view_ = data_[stripe_index_]->data() + stripe_offset_ * row_size_ + col_offset_;
            return view_;
        }

        uint64_t pos() override {
            return row_index_;
        }
    };

    unique_ptr<ColumnIterator> MemFlexBlock::col(uint32_t col_index) {
        return unique_ptr<ColumnIterator>(
                new FlexColumnIterator(memory_, stripe_size_, row_size_, col_offset_[col_index],
                                       col_offset_[col_index + 1] - col_offset_[col_index]));
    }

    class FlexRowIterator : public DataRowIterator {
    private:
        vector<shared_ptr<vector<uint64_t>>> &data_;
        MemDataRowPointer reference_;
        uint32_t stripe_size_;
        uint32_t row_size_;
        uint64_t row_index_;
        uint64_t stripe_index_;
        uint64_t stripe_offset_;
    public:
        FlexRowIterator(vector<shared_ptr<vector<uint64_t>>> &data, const vector<uint32_t> &col_offset,
                        uint32_t stripe_size)
                : data_(data), reference_(col_offset), stripe_size_(stripe_size), row_size_(col_offset.back()),
                  row_index_(-1), stripe_index_(0),
                  stripe_offset_(-1) {}

        DataRow &operator[](uint64_t idx) override {
            row_index_ = idx;
            stripe_index_ = idx / stripe_size_;
            stripe_offset_ = idx % stripe_size_;
            reference_.raw(data_[stripe_index_]->data() + stripe_offset_ * row_size_);
            return reference_;
        }

        DataRow &next() override {
            ++row_index_;
            ++stripe_offset_;
            if (stripe_offset_ >= stripe_size_) {
                stripe_index_ += 1;
                stripe_offset_ = 0;
            }
            reference_.raw(data_[stripe_index_]->data() + stripe_offset_ * row_size_);
            return reference_;
        }

        uint64_t pos() override {
            return row_index_;
        }
    };

    unique_ptr<DataRowIterator> MemFlexBlock::rows() {
        return unique_ptr<DataRowIterator>(new FlexRowIterator(memory_, col_offset_, stripe_size_));
    }

    MaskedBlock::MaskedBlock(shared_ptr<Block> inner, shared_ptr<Bitmap> mask)
            : inner_(inner), mask_(mask) {}

    uint64_t MaskedBlock::size() {
        return mask_->cardinality();
    }

    uint64_t MaskedBlock::limit() {
        return inner_->limit();
    }

    class MaskedColumnIterator : public ColumnIterator {
    private:
        unique_ptr<ColumnIterator> inner_;
        unique_ptr<BitmapIterator> bite_;
    public:
        MaskedColumnIterator(unique_ptr<ColumnIterator> inner, unique_ptr<BitmapIterator> bite)
                : inner_(move(inner)), bite_(move(bite)) {}

        DataField &operator[](uint64_t index) override {
            return (*inner_)[index];
        }

        DataField &next() override {
            return (*inner_)[bite_->next()];
        }

        uint64_t pos() override {
            return inner_->pos();
        }
    };

    unique_ptr<ColumnIterator> MaskedBlock::col(uint32_t col_index) {
        return unique_ptr<MaskedColumnIterator>(new MaskedColumnIterator(inner_->col(col_index),
                                                                         mask_->iterator()));
    }

    class MaskedRowIterator : public DataRowIterator {
    private:
        unique_ptr<DataRowIterator> inner_;
        unique_ptr<BitmapIterator> bite_;
    public:
        MaskedRowIterator(unique_ptr<DataRowIterator> inner, unique_ptr<BitmapIterator> bite)
                : inner_(move(inner)), bite_(move(bite)) {}

        virtual DataRow &operator[](uint64_t index) override {
            return (*inner_)[index];
        }

        virtual DataRow &next() override {
            return (*inner_)[bite_->next()];
        }

        uint64_t pos() override {
            return inner_->pos();
        }
    };

    unique_ptr<DataRowIterator> MaskedBlock::rows() {
        return unique_ptr<MaskedRowIterator>(new MaskedRowIterator(inner_->rows(), mask_->iterator()));
    }

    shared_ptr<Block> MaskedBlock::mask(shared_ptr<Bitmap> mask) {
        /// This sequence makes sure that we do not change the original mask, which may be shared by
        /// other instances of MaskedBlock
        this->mask_ = (*mask) & (*this->mask_);
        return this->shared_from_this();
    }

    ParquetBlock::ParquetBlock(ParquetTable *owner, shared_ptr<RowGroupReader> rowGroup, uint32_t index,
                               uint64_t columns) : Block(index), owner_(owner), rowGroup_(rowGroup), index_(index),
                                                   columns_(columns) {}

    Table *ParquetBlock::owner() {
        return this->owner_;
    }

    uint64_t ParquetBlock::size() {
        return rowGroup_->metadata()->num_rows();
    }

    class ParquetRowIterator;

#define COL_BUF_SIZE 8
    const int8_t WIDTH[8] = {1, sizeof(int32_t), sizeof(int64_t), 0, sizeof(float), sizeof(double), sizeof(ByteArray),
                             0};
    const int8_t SIZE[8] = {1, 1, 1, 1, 1, 1, sizeof(ByteArray) >> 3, 0};

    class ParquetColumnIterator : public ColumnIterator {
    private:
        shared_ptr<ColumnReader> columnReader_;
        DataField dataField_;
        DataField rawField_;
        int64_t buffer_size_;
        int64_t pos_;
        int64_t bufpos_;
        uint8_t width_;
        uint8_t *buffer_;
    public:
        ParquetColumnIterator(shared_ptr<ColumnReader> colReader)
                : columnReader_(colReader), dataField_(), rawField_(),
                  buffer_size_(0), pos_(-1), bufpos_(-8) {
            buffer_ = (uint8_t *) malloc(sizeof(ByteArray) * COL_BUF_SIZE);
            width_ = WIDTH[columnReader_->type()];
            dataField_.size_ = SIZE[columnReader_->type()];
            rawField_.size_ = 1;
        }

        virtual ~ParquetColumnIterator() {
            free(buffer_);
        }

        virtual DataField &operator[](uint64_t idx) override {
            uint64_t *pointer = loadBuffer(idx);
            pos_ = idx;
            dataField_ = pointer;
            return dataField_;
        }

        virtual DataField &operator()(uint64_t idx) override {
            uint64_t *pointer = loadBufferRaw(idx);
            pos_ = idx;
            rawField_ = pointer;
            return rawField_;
        }

        virtual DataField &next() override {
            return (*this)[pos_ + 1];
        }

        uint64_t pos() override {
            return pos_;
        }

    protected:
        inline uint64_t *loadBuffer(uint64_t idx) {
            if ((int64_t) idx < bufpos_ + buffer_size_) {
                return (uint64_t *) (buffer_ + width_ * (idx - bufpos_));
            } else {
                columnReader_->MoveTo(idx);
                columnReader_->ReadBatch(COL_BUF_SIZE, nullptr, nullptr, buffer_, &buffer_size_);
                bufpos_ = idx;
                return (uint64_t *) buffer_;
            }
        }

        inline uint64_t *loadBufferRaw(uint64_t idx) {
            if ((int64_t) idx < bufpos_ + buffer_size_) {
                return (uint64_t *) (buffer_ + sizeof(int32_t) * (idx - bufpos_));
            } else {
                columnReader_->MoveTo(idx);
                columnReader_->ReadBatchRaw(COL_BUF_SIZE, reinterpret_cast<uint32_t *>(buffer_), &buffer_size_);
                bufpos_ = idx;

                return (uint64_t *) buffer_;
            }
        }
    };

    class ParquetRowView : public DataRow {
    protected:
        vector<unique_ptr<ParquetColumnIterator>> &columns_;
        uint64_t index_;
        friend ParquetRowIterator;
    public:
        ParquetRowView(vector<unique_ptr<ParquetColumnIterator>> &cols) : columns_(cols), index_(-1) {}

        virtual ~ParquetRowView() = default;

        virtual DataField &operator[](uint64_t colindex) override {
            return (*(columns_[colindex]))[index_];
        }

        virtual DataField &operator()(uint64_t colindex) override {
            return (*(columns_[colindex]))(index_);
        }

        unique_ptr<DataRow> snapshot() override {
            return nullptr;
        }

        DataRow &operator=(DataRow &) override { return *this; }
    };

    template<typename DTYPE>
    shared_ptr<Bitmap> ParquetBlock::raw(uint32_t col_index, RawAccessor<DTYPE> *accessor) {
        accessor->init(this->size());
        auto pageReader = rowGroup_->GetColumnPageReader(col_index);
        shared_ptr<Page> page = pageReader->NextPage();

        if (page->type() == PageType::DICTIONARY_PAGE) {
            Dictionary<DTYPE> dict(static_pointer_cast<DictionaryPage>(page));
            accessor->dict(dict);
        } else {
            accessor->data((DataPage *) page.get());
        }
        while ((page = pageReader->NextPage())) {
            accessor->data((DataPage *) page.get());
        }
        return accessor->result();
    }

    unique_ptr<parquet::PageReader> ParquetBlock::pages(uint32_t col_index) {
        return rowGroup_->GetColumnPageReader(col_index);
    }

    class ParquetRowIterator : public DataRowIterator {
    private:
        vector<unique_ptr<ParquetColumnIterator>> columns_;
        ParquetRowView view_;
    public:
        ParquetRowIterator(ParquetBlock &block, uint64_t colindices)
                : columns_(64 - __builtin_clzl(colindices)), view_(columns_) {
            Bitset bitset(colindices);
            while (bitset.hasNext()) {
                auto index = bitset.next();
                columns_[index] = unique_ptr<ParquetColumnIterator>(
                        (ParquetColumnIterator *) (block.col(index).release()));
            }
        }

        virtual ~ParquetRowIterator() {
            columns_.clear();
        }

        virtual DataRow &operator[](uint64_t index) override {
            view_.index_ = index;
            return view_;
        }

        virtual DataRow &next() override {
            view_.index_++;
            return view_;
        }

        uint64_t pos() override {
            return view_.index_;
        }

    };

    unique_ptr<DataRowIterator> ParquetBlock::rows() {
        return unique_ptr<DataRowIterator>(new ParquetRowIterator(*this, columns_));
    }

    unique_ptr<ColumnIterator> ParquetBlock::col(uint32_t col_index) {
        return unique_ptr<ColumnIterator>(new ParquetColumnIterator(rowGroup_->Column(col_index)));
    }

    shared_ptr<Block> ParquetBlock::mask(shared_ptr<Bitmap> mask) {
        return make_shared<MaskedBlock>(dynamic_pointer_cast<ParquetBlock>(this->shared_from_this()), mask);
    }

    uint64_t Table::size() {
        function<uint64_t(const shared_ptr<Block> &)> sizer = [](const shared_ptr<Block> &block) {
            return block->size();
        };
        function<uint64_t(uint64_t, uint64_t)> reducer = [](uint64_t a, uint64_t b) {
            return a + b;
        };
        return blocks()->map(sizer)->reduce(reducer);
    }

    ParquetTable::ParquetTable(const string &fileName, uint64_t columns)
            : name_(fileName), columns_(columns) {
        type_ = EXTERNAL;
        fileReader_ = ParquetFileReader::OpenFile(fileName);
        if (!fileReader_) {
            throw std::invalid_argument("ParquetTable-Open: file not found");
        }
    }

    void ParquetTable::updateColumns(uint64_t columns) {
        columns_ = columns;
    }

    shared_ptr<ParquetTable> ParquetTable::Open(const string &filename, uint64_t columns) {
        return make_shared<ParquetTable>(filename, columns);
    }

    shared_ptr<ParquetTable> ParquetTable::Open(const string &filename, std::initializer_list<uint32_t> columns) {
        uint64_t ccs = 0;
        for (uint32_t c:columns) {
            ccs |= 1ul << c;
        }
        return Open(filename, ccs);
    }

    using namespace std::placeholders;

    unique_ptr<Stream<shared_ptr<Block>>> ParquetTable::blocks() {
        function<shared_ptr<Block>(const int &)> mapper = bind(&ParquetTable::createParquetBlock, this, _1);
        uint32_t numRowGroups = fileReader_->metadata()->num_row_groups();
#ifdef LQF_PARALLEL
        auto stream = IntStream::Make(0, numRowGroups)->parallel()->map(mapper);
#else
        auto stream = IntStream::Make(0, numRowGroups)->map(mapper);
#endif
        return stream;
    }

    const vector<uint32_t> &ParquetTable::colSize() {
        return lqf::colSize(0);
    }

    shared_ptr<ParquetBlock> ParquetTable::createParquetBlock(const int &block_idx) {
        auto rowGroup = fileReader_->RowGroup(block_idx);
        return make_shared<ParquetBlock>(this, rowGroup, block_idx, columns_);
    }

    template<typename DTYPE>
    unique_ptr<Dictionary<DTYPE>> ParquetTable::LoadDictionary(int column) {
        auto dictpage = static_pointer_cast<DictionaryPage>(
                fileReader_->RowGroup(0)->GetColumnPageReader(column)->NextPage());
        return unique_ptr<Dictionary<DTYPE>>(new Dictionary<DTYPE>(dictpage));
    }

    MaskedTable::MaskedTable(ParquetTable *inner, vector<shared_ptr<Bitmap>> &masks)
            : inner_(inner), masks_(masks) {
        type_ = inner->type();
        masks_.resize(inner_->numBlocks());
    }

    using namespace std::placeholders;

    unique_ptr<Stream<shared_ptr<Block>>> MaskedTable::blocks() {
        function<shared_ptr<Block>(const shared_ptr<Block> &)> mapper =
                bind(&MaskedTable::buildMaskedBlock, this, _1);
        return inner_->blocks()->map(mapper);
    }

    const vector<uint32_t> &MaskedTable::colSize() {
        return inner_->colSize();
    }

    shared_ptr<Block> MaskedTable::buildMaskedBlock(const shared_ptr<Block> &input) {
        auto pblock = dynamic_pointer_cast<ParquetBlock>(input);
        return make_shared<MaskedBlock>(pblock, masks_[pblock->index()]);
    }

    TableView::TableView(TABLE_TYPE type, const vector<uint32_t> &col_size,
                         unique_ptr<Stream<shared_ptr<Block>>> stream)
            : col_size_(col_size), stream_(move(stream)) {
        type_ = type;
    }

    const vector<uint32_t> &TableView::colSize() {
        return col_size_;
    }

    unique_ptr<Stream<shared_ptr<Block>>> TableView::blocks() {
        return move(stream_);
    }

    shared_ptr<MemTable> MemTable::Make(uint8_t num_fields, bool vertical) {
        return make_shared<MemTable>(lqf::colSize(num_fields), vertical);
    }

    shared_ptr<MemTable> MemTable::Make(const vector<uint32_t> col_size, bool vertical) {
        return make_shared<MemTable>(col_size, vertical);
    }

    MemTable::MemTable(const vector<uint32_t> col_size, bool vertical)
            : vertical_(vertical), col_size_(col_size), row_size_(0),
              col_offset_(lqf::size2offset(col_size)), blocks_(vector<shared_ptr<Block>>()) {
        type_ = vertical_ ? OTHER : RAW;
        row_size_ = col_offset_.back();
    }

    shared_ptr<Block> MemTable::allocate(uint32_t num_rows) {
        shared_ptr<Block> block;
        if (vertical_)
            block = make_shared<MemvBlock>(num_rows, col_size_);
        else
            block = make_shared<MemBlock>(num_rows, col_offset_);

        std::unique_lock lock(write_lock_);
        blocks_.push_back(block);
        lock.unlock();

        return block;
    }

    shared_ptr<MemFlexBlock> MemTable::allocateFlex() {
        if (vertical_)
            return nullptr;
        auto flex = make_shared<MemFlexBlock>(col_offset_);
        std::unique_lock lock(write_lock_);
        blocks_.push_back(flex);

        return flex;
    }

    void MemTable::append(shared_ptr<Block> block) {
        std::lock_guard lock(write_lock_);
        blocks_.push_back(block);
    }

    unique_ptr<Stream<shared_ptr<Block>>> MemTable::blocks() {
#ifdef LQF_PARALLEL
        return unique_ptr<Stream<shared_ptr<Block>>>(new VectorStream<shared_ptr<Block>>(blocks_))->parallel();
#else
        return unique_ptr<Stream<shared_ptr<Block>>>(new VectorStream<shared_ptr<Block>>(blocks_));
#endif
    }

    const vector<uint32_t> &MemTable::colSize() { return col_size_; }

    const vector<uint32_t> &MemTable::colOffset() { return col_offset_; }

/**
 * Initialize the templates
 */

    template
    class RawAccessor<Int32Type>;

    template
    class RawAccessor<DoubleType>;

    template
    class RawAccessor<ByteArrayType>;

    template shared_ptr<Bitmap>
    ParquetBlock::raw<Int32Type>(uint32_t col_index, RawAccessor<Int32Type> *accessor);

    template shared_ptr<Bitmap>
    ParquetBlock::raw<DoubleType>(uint32_t col_index, RawAccessor<DoubleType> *accessor);

    template shared_ptr<Bitmap>
    ParquetBlock::raw<ByteArrayType>(uint32_t col_index, RawAccessor<ByteArrayType> *accessor);

    template unique_ptr<Dictionary<Int32Type>> ParquetTable::LoadDictionary<Int32Type>(int index);

    template unique_ptr<Dictionary<DoubleType>> ParquetTable::LoadDictionary<DoubleType>(int index);

    template unique_ptr<Dictionary<ByteArrayType>> ParquetTable::LoadDictionary<ByteArrayType>(int index);

}