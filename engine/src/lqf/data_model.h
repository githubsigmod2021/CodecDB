//
// Created by harper on 2/9/20.
//

#ifndef CHIDATA_LQF_DATA_MODEL_H
#define CHIDATA_LQF_DATA_MODEL_H

#include <cstdint>
#include <random>
#include <iostream>
#include <arrow/util/bit_stream_utils.h>
#include <parquet/file_reader.h>
#include <parquet/column_reader.h>
#include <parquet/column_page.h>
#include "stream.h"
#include "bitmap.h"
#include "dict.h"
#include "parallel.h"

namespace lqf {

    using namespace std;
    using namespace parquet;


    const vector<uint32_t> &colOffset(uint32_t num_fields);

    const vector<uint32_t> &colSize(uint32_t num_fields);

    vector<uint32_t> offset2size(const vector<uint32_t> &offset);

    vector<uint32_t> size2offset(const vector<uint32_t> &size);

    union DataPointer {
        uint64_t *raw_;
        int32_t *ival_;
        double *dval_;
        ByteArray *sval_;
    };

    class DataField {
    public:
        DataPointer pointer_;
        uint8_t size_;

        virtual ~DataField() = default;

        inline int32_t asInt() const { return *pointer_.ival_; }

        inline double asDouble() const { return *pointer_.dval_; }

        inline ByteArray &asByteArray() const { return *pointer_.sval_; }

        inline void operator=(int32_t value) { *pointer_.ival_ = value; }

        inline void operator=(double value) { *pointer_.dval_ = value; }

        inline void operator=(ByteArray &value) { *pointer_.sval_ = value; }

        inline void operator=(uint64_t *raw) { pointer_.raw_ = raw; };

        inline void operator=(const uint8_t *raw) {
            assert(size_ <= 2);
            memcpy((void *) pointer_.raw_, (void *) raw, sizeof(uint64_t) * size_);
        };

        inline uint64_t *data() const { return pointer_.raw_; }

        inline void operator=(DataField &df) {
            assert(size_ <= 2);
            assert(df.size_ <= 2);
            if (df.size_ > size_) {
                return;
            }
            assert(df.size_ <= size_);
            // We allow the input size to be smaller than local size, allowing a larger field
            // to be used for smaller field
            memcpy((void *) pointer_.raw_, (void *) df.pointer_.raw_, sizeof(uint64_t) * df.size_);
//            *raw_ = *df.raw_;
        }
    };

    ostream &operator<<(ostream &os, DataField &dt);

    /*
     * A structure representing in-memory data fields
     */
    class DataRow {
    public:
        virtual ~DataRow() = default;

        virtual DataField &operator[](uint64_t i) = 0;

        virtual DataField &operator()(uint64_t i) {
            return (*this)[i];
        }

        virtual DataRow &operator=(DataRow &row) = 0;

        virtual uint32_t size() {
            return 0;
        }

        virtual uint32_t num_fields() {
            return 0;
        }

        virtual uint64_t *raw() {
            return nullptr;
        }

        // Create a snapshot of current row
        virtual unique_ptr<DataRow> snapshot() = 0;
    };

    class MemDataRow : public DataRow {
    private:
        vector<uint64_t> data_;
        const vector<uint32_t> &offset_;
        DataField view_;
    public:
        MemDataRow(uint8_t num_fields);

        MemDataRow(const vector<uint32_t> &offset);

        virtual ~MemDataRow() = default;

        DataField &operator[](uint64_t i) override;

        DataRow &operator=(DataRow &row) override;

        MemDataRow &operator=(MemDataRow &row);

        unique_ptr<DataRow> snapshot() override;

        inline uint32_t size() override { return data_.size(); }

        inline uint32_t num_fields() override { return offset_.size() - 1; }

        uint64_t *raw() override;

        inline const vector<uint32_t> &offset() const { return offset_; };

        static MemDataRow EMPTY;
    };

    ostream &operator<<(ostream &os, MemDataRow &dt);

    class DataRowIterator {
    public:
        virtual ~DataRowIterator() = default;

        virtual DataRow &operator[](uint64_t idx) = 0;

        virtual DataRow &next() = 0;

        virtual uint64_t pos() = 0;

    };

    class ColumnIterator {
    public:
        virtual ~ColumnIterator() = default;

        virtual DataField &next() = 0;

        virtual DataField &operator[](uint64_t idx) = 0;

        virtual DataField &operator()(uint64_t idx) {
            return (*this)[idx];
        }

        virtual uint64_t pos() = 0;
    };

    class Table;

    ///
    /// Each block will be processed by a thread
    ///
    class Block : public enable_shared_from_this<Block> {
    private:
        static mt19937 rand_;
    protected:
        uint32_t id_;
    public:
        Block(uint32_t id) : id_(id) {}

        Block() : Block(rand_()) {}

        virtual ~Block() = default;

        virtual Table *owner() { return nullptr; }

        inline uint32_t id() { return id_; };

        /// Number of rows in the block
        virtual uint64_t size() = 0;

        /// Block limit, used for creating bitmaps
        virtual uint64_t limit() { return size(); };

        virtual unique_ptr<ColumnIterator> col(uint32_t col_index) = 0;

        virtual unique_ptr<DataRowIterator> rows() = 0;

        virtual shared_ptr<Block> mask(shared_ptr<Bitmap> mask) = 0;

        virtual void resize(uint32_t newsize) {}
    };

    class MemBlock : public Block {
    private:
        uint32_t size_;
        uint32_t row_size_;
        const vector<uint32_t> &col_offset_;

        vector<uint64_t> content_;
    public:
        MemBlock(uint32_t size, const vector<uint32_t> &col_offset);

        MemBlock(uint32_t size, uint32_t row_size);

        virtual ~MemBlock() = default;

        uint64_t size() override;

        void resize(uint32_t newsize) override;

        inline vector<uint64_t> &content();

        unique_ptr<ColumnIterator> col(uint32_t col_index) override;

        unique_ptr<DataRowIterator> rows() override;

        shared_ptr<Block> mask(shared_ptr<Bitmap> mask) override;

        inline const vector<uint32_t> &col_offset() { return col_offset_; }
    };

    class MemvBlock : public Block {
    private:
        uint32_t size_;
        const vector<uint32_t> &col_size_;

        vector<unique_ptr<vector<uint64_t>>> content_;
    public:
        MemvBlock(uint32_t size, const vector<uint32_t> &col_size);

        MemvBlock(uint32_t size, uint32_t num_fields);

        virtual ~MemvBlock() = default;

        uint64_t size() override;

        void resize(uint32_t newsize) override;

        unique_ptr<ColumnIterator> col(uint32_t col_index) override;

        unique_ptr<DataRowIterator> rows() override;

        shared_ptr<Block> mask(shared_ptr<Bitmap> mask) override;

        void merge(MemvBlock &, const vector<pair<uint8_t, uint8_t>> &);
    };

#define FLEX_SLAB_SIZE_ 131072

    class MemDataRowPointer : public DataRow {
    private:
        uint64_t *pointer_;
        const vector<uint32_t> &offset_;
        DataField view_;
    public:
        MemDataRowPointer(const vector<uint32_t> &);

        virtual ~MemDataRowPointer() = default;

        DataField &operator[](uint64_t i) override;

        DataRow &operator=(DataRow &row) override;

        unique_ptr<DataRow> snapshot() override;

        uint64_t *raw() override;

        inline uint32_t size() override { return offset_.back(); }

        inline uint32_t num_fields() override { return offset_.size() - 1; }

        inline void raw(uint64_t *p) { pointer_ = p; }

        inline const vector<uint32_t> &offset() const { return offset_; };
    };

    class MemFlexBlock : public Block {
    private:
        uint32_t slab_size_;
        vector<shared_ptr<vector<uint64_t>>> memory_;
        uint32_t size_;
        uint32_t row_size_;
        uint32_t stripe_size_;
        vector<uint32_t> col_offset_;
        uint32_t pointer_;
        MemDataRowPointer accessor_;
    public:
        MemFlexBlock(const vector<uint32_t> &);

        MemFlexBlock(const vector<uint32_t> &, uint32_t);

        virtual ~MemFlexBlock() = default;

        uint64_t size() override;

        DataRow &push_back();

        DataRow &operator[](uint32_t index);

        void assign(vector<shared_ptr<vector<uint64_t>>> &content, uint32_t slab_size, uint32_t size);

        unique_ptr<ColumnIterator> col(uint32_t col_index) override;

        unique_ptr<DataRowIterator> rows() override;

        shared_ptr<Block> mask(shared_ptr<Bitmap> mask) override;

        inline const vector<uint32_t> &col_offset() { return col_offset_; }
    };

    template<typename DTYPE>
    class RawAccessor {
    protected:

        shared_ptr<SimpleBitmap> bitmap_;

        uint64_t offset_;

        virtual void scanPage(uint64_t numEntry, const uint8_t *data, uint64_t *bitmap, uint64_t bitmap_offset) {};

    public:
        RawAccessor() : offset_(0) {}

        virtual ~RawAccessor() = default;

        virtual void init(uint64_t size) {
            bitmap_ = make_shared<SimpleBitmap>(size);
            offset_ = 0;
        }

        virtual void dict(Dictionary<DTYPE> &dict) {}

        virtual void data(DataPage *dpage) {
            // Assume all fields are mandatory, which is true for TPCH
            scanPage(dpage->num_values(), dpage->data(), bitmap_->raw(), offset_);
            offset_ += dpage->num_values();
        }

        virtual shared_ptr<Bitmap> result() {
            return bitmap_;
        }
    };

    using Int32Accessor = RawAccessor<Int32Type>;
    using DoubleAccessor = RawAccessor<DoubleType>;
    using ByteArrayAccessor = RawAccessor<ByteArrayType>;

    class ParquetTable;

    class ParquetBlock : public Block {
    protected:
        ParquetTable *owner_;
        shared_ptr<RowGroupReader> rowGroup_;
        uint32_t index_;
        uint64_t columns_;
    public:
        ParquetBlock(ParquetTable *, shared_ptr<RowGroupReader>, uint32_t, uint64_t);

        virtual ~ParquetBlock() = default;

        uint64_t size() override;

        inline uint32_t index() { return index_; }

        Table *owner() override;

        template<typename DTYPE>
        shared_ptr<Bitmap> raw(uint32_t col_index, RawAccessor<DTYPE> *accessor);

        unique_ptr<parquet::PageReader> pages(uint32_t);

        unique_ptr<ColumnIterator> col(uint32_t col_index) override;

        unique_ptr<DataRowIterator> rows() override;

        shared_ptr<Block> mask(shared_ptr<Bitmap> mask) override;

    };

    class MaskedBlock : public Block {
        shared_ptr<Block> inner_;
        shared_ptr<Bitmap> mask_;
    public:
        MaskedBlock(shared_ptr<Block> inner, shared_ptr<Bitmap> mask);

        virtual ~MaskedBlock() = default;

        uint64_t size() override;

        uint64_t limit() override;

        inline shared_ptr<Block> inner() { return inner_; }

        inline shared_ptr<Bitmap> mask() { return mask_; }

        unique_ptr<ColumnIterator> col(uint32_t col_index) override;

        unique_ptr<DataRowIterator> rows() override;

        shared_ptr<Block> mask(shared_ptr<Bitmap> mask) override;
    };

    enum TABLE_TYPE {
        RAW, EXTERNAL, OTHER
    };

    class Table {
    protected:
        TABLE_TYPE type_ = OTHER;
    public:
        virtual unique_ptr<Stream<shared_ptr<Block>>> blocks() = 0;

        /**
         * The number of columns in the table
         * @return
         */
        virtual const vector<uint32_t> &colSize() = 0;

        virtual uint64_t size();

        inline TABLE_TYPE type() { return type_; }
    };

    class ParquetTable : public Table {
    private:
        const string name_;

        uint64_t columns_;

        unique_ptr<ParquetFileReader> fileReader_;
    public:
        ParquetTable(const string &fileName, uint64_t columns = 0);

        virtual ~ParquetTable() = default;

        virtual unique_ptr<Stream<shared_ptr<Block>>> blocks() override;

        const vector<uint32_t> &colSize() override;

        void updateColumns(uint64_t columns);

        inline uint64_t size() override { return fileReader_->metadata()->num_rows(); }

        inline uint32_t numBlocks() { return fileReader_->metadata()->num_row_groups(); }

        template<typename DTYPE>
        unique_ptr<Dictionary<DTYPE>> LoadDictionary(int column);

        static shared_ptr<ParquetTable> Open(const string &filename, uint64_t columns = 0);

        static shared_ptr<ParquetTable> Open(const string &filename, std::initializer_list<uint32_t> columns);

    protected:
        shared_ptr<ParquetBlock> createParquetBlock(const int &block_idx);

    };

    class MaskedTable : public Table {
    private:
        ParquetTable *inner_;
        vector<shared_ptr<Bitmap>> masks_;
    public:

        MaskedTable(ParquetTable *, vector<shared_ptr<Bitmap>> &);

        virtual ~MaskedTable() = default;

        virtual unique_ptr<Stream<shared_ptr<Block>>> blocks() override;

        const vector<uint32_t> &colSize() override;

    protected:
        shared_ptr<Block> buildMaskedBlock(const shared_ptr<Block> &);
    };

    class TableView : public Table {
        vector<uint32_t> col_size_;
        unique_ptr<Stream<shared_ptr<Block>>> stream_;
    public:
        TableView(TABLE_TYPE, const vector<uint32_t> &, unique_ptr<Stream<shared_ptr<Block>>>);

        virtual ~TableView() = default;

        unique_ptr<Stream<shared_ptr<Block>>> blocks() override;

        const vector<uint32_t> &colSize() override;

    };

    class MemTable : public Table {
    protected:
        bool vertical_;

        // For columnar view
        vector<uint32_t> col_size_;

        // For row view
        uint32_t row_size_;
        vector<uint32_t> col_offset_;

        vector<shared_ptr<Block>> blocks_;

        mutex write_lock_;
    public:
        MemTable(const vector<uint32_t> col_size, bool vertical);

        static shared_ptr<MemTable> Make(uint8_t num_fields, bool vertical = false);

        static shared_ptr<MemTable> Make(const vector<uint32_t> col_size, bool vertical = false);

        virtual ~MemTable() = default;

        shared_ptr<Block> allocate(uint32_t num_rows);

        shared_ptr<MemFlexBlock> allocateFlex();

        void append(shared_ptr<Block>);

        unique_ptr<Stream<shared_ptr<Block>>> blocks() override;

        const vector<uint32_t> &colSize() override;

        const vector<uint32_t> &colOffset();

        inline bool isVertical() { return vertical_; }
    };

    using TableOutput = parallel::TypedOutput<shared_ptr<Table>>;
    using TableNode = parallel::WrapperNode<shared_ptr<Table>>;
}

#endif //CHIDATA_LQF_DATA_MODEL_H
