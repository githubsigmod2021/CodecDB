//
// Created by harper on 2/21/20.
//

#include "filter.h"
#include "filter_executor.h"
#include <sboost/encoding/rlehybrid.h>
#include <functional>
#include <sboost/sboost.h>
#include <sboost/simd.h>
#include <sboost/bitmap_writer.h>
#include <sboost/encoding/encoding_utils.h>

using namespace std;
using namespace std::placeholders;
using namespace sboost;
using namespace sboost::encoding;

namespace lqf {

    Filter::Filter() : Node(1, true) {}

    unique_ptr<NodeOutput> Filter::execute(const vector<NodeOutput *> &inputs) {
        auto input0 = static_cast<TableOutput *>(inputs[0]);
        auto table = filter(*(input0->get()));
        return unique_ptr<TableOutput>(new TableOutput(table));
    }

    shared_ptr<Table> Filter::filter(Table &input) {
        function<shared_ptr<Block>(
                const shared_ptr<Block> &)> mapper = bind(&Filter::processBlock, this, _1);
        return make_shared<TableView>(input.type(), input.colSize(), input.blocks()->map(mapper));
    }

    shared_ptr<Block> Filter::processBlock(const shared_ptr<Block> &input) {
        shared_ptr<Bitmap> result = filterBlock(*input);
        return input->mask(result);
    }

    ColPredicate::ColPredicate(uint32_t index) : index_(index) {}

    SimplePredicate::SimplePredicate(uint32_t index, function<bool(const DataField &)> pred)
            : ColPredicate(index), predicate_(pred) {}

    void SimplePredicate::predicate(function<bool(const DataField &)> f) {
        this->predicate_ = f;
    }

    shared_ptr<Bitmap> SimplePredicate::filterBlock(Block &block, Bitmap &skip) {
        auto result = make_shared<SimpleBitmap>(block.limit());

        auto ite = block.col(index_);
        if (skip.isFull()) {
            auto block_size = block.size();
            for (uint64_t i = 0; i < block_size; ++i) {
                if (predicate_(ite->next())) {
                    result->put(ite->pos());
                }
            }
        } else {
            auto posite = skip.iterator();
            while (posite->hasNext()) {
                auto pos = posite->next();
                if (predicate_((*ite)[pos])) {
                    result->put(pos);
                }
            }
        }
        return result;
    }

    ColFilter::ColFilter(ColPredicate *pred) {
        predicates_.push_back(unique_ptr<ColPredicate>(pred));
    }

    ColFilter::ColFilter(initializer_list<ColPredicate *> preds) {
        for (auto &pred: preds) {
            predicates_.push_back(unique_ptr<ColPredicate>(pred));
        }
    }

    ColFilter::~ColFilter() { predicates_.clear(); }

    ColPredicate *ColFilter::predicate(uint32_t index) {
        return predicates_[index].get();
    }

    shared_ptr<Table> ColFilter::filter(Table &input) {
        for (auto &pred: predicates_) {
            FilterExecutor::inst->reg(input, *pred);
        }
        return Filter::filter(input);
    }

    shared_ptr<Bitmap> ColFilter::filterBlock(Block &input) {
        shared_ptr<Bitmap> result = make_shared<FullBitmap>(input.limit());
        for (auto &pred: predicates_) {
            result = pred->filterBlock(input, *result);
        }
        return result;
    }

    RowFilter::RowFilter(function<bool(DataRow &)> pred) : predicate_(pred) {}

    shared_ptr<Bitmap> RowFilter::filterBlock(Block &input) {
        auto result = make_shared<SimpleBitmap>(input.limit());
        auto rit = input.rows();

        auto block_size = input.size();
        for (uint32_t i = 0; i < block_size; ++i) {
            if (predicate_(rit->next())) {
                result->put(rit->pos());
            }
        }
        return result;
    }

    using namespace ::sboost::encoding::rlehybrid;

    class PagedSegmentReader {
    protected:
        PageReader *page_reader_;
        unique_ptr<SegmentReader> inpage_reader_;
        shared_ptr<Page> current_page_;
        shared_ptr<Page> next_page_;
        uint32_t bit_width_;

        void initInPageReader() {
            auto datapage = static_pointer_cast<DataPage>(current_page_);
            bit_width_ = datapage->data()[0];
            inpage_reader_ = unique_ptr<SegmentReader>(
                    new SegmentReader(datapage->data() + 1, bit_width_, datapage->num_values()));
        }

    public:
        PagedSegmentReader(PageReader *page_reader) : page_reader_(page_reader) {
            // Skip Dictionary Page
            page_reader_->NextPage();

            current_page_ = page_reader_->NextPage();
            next_page_ = page_reader_->NextPage();
            initInPageReader();
        }

        bool hasNext() {
            bool currentPageNext = inpage_reader_->hasNext();
            if (currentPageNext) {
                return true;
            }
            return next_page_ != nullptr;
        }

        Segment next() {
            if (inpage_reader_->hasNext()) {
                return inpage_reader_->next();
            }
            current_page_ = move(next_page_);
            next_page_ = page_reader_->NextPage();
            initInPageReader();
            return inpage_reader_->next();
        }

        uint32_t bitWidth() {
            return bit_width_;
        }
    };

    SboostRowFilter::SboostRowFilter(uint32_t col1, uint32_t col2)
            : column1_(col1), column2_(col2) {}

    /**
     * Make sure the returned value is aligned
     *
     */
    const uint8_t *align_buffer(const uint8_t *source, uint8_t *dest, uint32_t bit_width,
                                uint32_t length, uint32_t entryoff) {
        uint32_t num_bits = bit_width * entryoff;
        uint32_t num_bytes = num_bits >> 3;
        if ((num_bits & 0x7) == 0) {
            return source + num_bytes;
        }
        uint64_t *source_view = (uint64_t *) (source + num_bytes);
        uint64_t *dest_view = (uint64_t *) dest;
        uint32_t offset = num_bits & 0x7;
        uint32_t noffset = 64 - offset;
        uint32_t pointer = num_bytes;
        uint32_t counter = 0;
        while (pointer < length) {
            dest_view[counter] = (source_view[counter] << offset) | ((source_view[counter + 1]) >> noffset);
            pointer += 8;
            counter++;
        }
        return dest;
    }

    shared_ptr<Bitmap> SboostRowFilter::filterBlock(Block &block) {
        auto parquetBlock = static_cast<ParquetBlock &>(block);
        auto pages1 = parquetBlock.pages(column1_);
        auto pages2 = parquetBlock.pages(column2_);

        auto num_entry = block.size();
        shared_ptr<SimpleBitmap> result = make_shared<SimpleBitmap>(num_entry);
        BitmapWriter bitmapWriter(result->raw(), 0);

        PagedSegmentReader sreader1(pages1.get());
        PagedSegmentReader sreader2(pages2.get());

        Segment seg1 = sreader1.next();
        Segment seg2 = sreader2.next();
        uint32_t remain1 = seg1.num_entry_;
        uint32_t remain2 = seg2.num_entry_;

        // Load PACKED data in buffer
        uint8_t *align_buffer1 = (uint8_t *) aligned_alloc(64, sizeof(uint8_t) * 8192);
        uint8_t *align_buffer2 = (uint8_t *) aligned_alloc(64, sizeof(uint8_t) * 8192);

        uint32_t processed = 0;

        auto bitwidth = sreader1.bitWidth();
        ::sboost::BitpackCompare bpcompare(bitwidth);

        while (processed < num_entry) {
            auto remain = std::min(remain1, remain2);
            switch ((seg1.mode_ << 1) + seg2.mode_) {
                case 0:   // left RLE, right RLE
                    bitmapWriter.appendBits(seg1.value_ < seg2.value_, remain);
                    break;
                case 1: { // left RLE, right PACKED
                    ::sboost::Bitpack bitpack(seg1.value_, bitwidth);
                    // Align seg2 data
                    auto buffer2_aligned = align_buffer(seg2.data_, align_buffer2, bitwidth,
                                                        seg2.data_length_, seg2.num_entry_ - remain2);
                    bitpack.greater(buffer2_aligned, remain, result->raw(), bitmapWriter.offset());
                    bitmapWriter.moveForward(remain);
                }
                    break;
                case 2: { // left PACKED, right RLE
                    ::sboost::Bitpack bitpack(seg2.value_, bitwidth);
                    // Align seg1 data
                    auto buffer1_aligned = align_buffer(seg1.data_, align_buffer1, bitwidth,
                                                        seg1.data_length_, seg1.num_entry_ - remain1);
                    bitpack.less(buffer1_aligned, remain, result->raw(), bitmapWriter.offset());
                    bitmapWriter.moveForward(remain);
                }
                    break;
                case 3: // left PACKED, right PACKED
                    auto buffer1_aligned = align_buffer(seg1.data_, align_buffer1, bitwidth,
                                                        seg1.data_length_, seg1.num_entry_ - remain1);
                    auto buffer2_aligned = align_buffer(seg2.data_, align_buffer2, bitwidth,
                                                        seg2.data_length_, seg2.num_entry_ - remain2);
                    bpcompare.less(buffer1_aligned, buffer2_aligned,
                                   remain, result->raw(), bitmapWriter.offset());
                    bitmapWriter.moveForward(remain);
                    break;
            }
            processed += remain;
            if (remain == remain1 && sreader1.hasNext()) {
                seg1 = sreader1.next();
                remain1 = seg1.num_entry_;
            }
            if (remain == remain2 && sreader2.hasNext()) {
                seg2 = sreader2.next();
                remain2 = seg2.num_entry_;
            }
        }
        ::sboost::encoding::cleanup(processed, num_entry, result->raw(), 0);

        free(align_buffer1);
        free(align_buffer2);

        return result;
    }

    SboostRow2Filter::SboostRow2Filter(uint32_t col1, uint32_t col2, uint32_t col3)
            : column1_(col1), column2_(col2), column3_(col3) {}

    shared_ptr<Bitmap> SboostRow2Filter::filterBlock(Block &block) {
        auto parquetBlock = static_cast<ParquetBlock &>(block);
        auto pages1 = parquetBlock.pages(column1_);
        auto pages2 = parquetBlock.pages(column2_);
        auto pages3 = parquetBlock.pages(column3_);

        auto num_entry = block.size();
        shared_ptr<SimpleBitmap> result = make_shared<SimpleBitmap>(num_entry);
        BitmapWriter bitmapWriter(result->raw(), 0);

        PagedSegmentReader sreader1(pages1.get());
        PagedSegmentReader sreader2(pages2.get());
        PagedSegmentReader sreader3(pages3.get());

        Segment seg1 = sreader1.next();
        Segment seg2 = sreader2.next();
        Segment seg3 = sreader3.next();

        uint32_t processed = 0;

        ::sboost::BitpackCompare bpcompare(sreader1.bitWidth());

        uint64_t *cmp12 = (uint64_t *) aligned_alloc(64, 20 * sizeof(uint64_t));
        uint64_t *cmp23 = (uint64_t *) aligned_alloc(64, 20 * sizeof(uint64_t));

        while (processed < num_entry) {
            memset(cmp12, 0, 20 * sizeof(uint64_t));
            memset(cmp23, 0, 20 * sizeof(uint64_t));
            assert(seg1.num_entry_ == seg2.num_entry_);
            assert(seg2.num_entry_ == seg3.num_entry_);
            assert(seg1.mode_ == PACKED);
            assert(seg2.mode_ == PACKED);
            assert(seg3.mode_ == PACKED);

            auto num_words = (seg1.num_entry_ + 63) >> 6;
            bpcompare.less(seg1.data_, seg2.data_, seg1.num_entry_, cmp12, 0);
            bpcompare.less(seg2.data_, seg3.data_, seg2.num_entry_, cmp23, 0);
            ::sboost::simd::simd_and(cmp12, cmp23, num_words);

            bitmapWriter.appendWord(cmp12, seg1.num_entry_);
            processed += seg1.num_entry_;
            if (sreader1.hasNext())
                seg1 = sreader1.next();
            if (sreader2.hasNext())
                seg2 = sreader2.next();
            if (sreader3.hasNext())
                seg3 = sreader3.next();
        }
        auto full_size = ((num_entry >> 6) + 1) << 6;
        ::sboost::encoding::cleanup(full_size, num_entry, result->raw(), 0);

        free(cmp12);
        free(cmp23);
        return result;
    }

    KeyFinder::KeyFinder(uint32_t key_index, function<bool(DataRow &)> pred)
            : key_index_(key_index), predicate_(pred) {}

    int32_t KeyFinder::find(Table &table) {
        function<int32_t(const shared_ptr<Block> &)> mapper = bind(&KeyFinder::filterBlock, this,
                                                                   placeholders::_1);
        auto values = table.blocks()->map(mapper)->collect();
        return (*values)[0];
    }

    int32_t KeyFinder::filterBlock(const shared_ptr<Block> &block) {
        auto rows = block->rows();
        auto block_size = block->size();
        for (uint32_t i = 0; i < block_size; ++i) {
            DataRow &row = rows->next();
            if (predicate_(row)) {
                return row[key_index_].asInt();
            }
        }
        return -1;
    }

    MapFilter::MapFilter(uint32_t key_index) : key_index_(key_index) {}

    MapFilter::MapFilter(uint32_t key_index, IntPredicate<Int32> &map)
            : key_index_(key_index), map_(&map) {}

    void MapFilter::setMap(IntPredicate<Int32> &map) {
        map_ = &map;
    }

    shared_ptr<Bitmap> MapFilter::filterBlock(Block &input) {
        auto col = input.col(key_index_);
        auto bitmap = make_shared<SimpleBitmap>(input.limit());
        auto block_size = input.size();
        for (uint32_t i = 0; i < block_size; ++i) {
            if (map_->test(col->next().asInt())) {
                bitmap->put(i);
            }
        }
        return bitmap;
    }

    PowerMapFilter::PowerMapFilter(function<uint64_t(DataRow &)> key_maker) : key_maker_(key_maker) {}

    PowerMapFilter::PowerMapFilter(function<uint64_t(DataRow &)> key_maker, IntPredicate<Int64> &map)
            : key_maker_(key_maker), map_(&map) {}

    void PowerMapFilter::setMap(IntPredicate<Int64> &map) {
        map_ = &map;
    }

    shared_ptr<Bitmap> PowerMapFilter::filterBlock(Block &input) {
        auto rows = input.rows();
        auto bitmap = make_shared<SimpleBitmap>(input.limit());
        auto block_size = input.size();
        for (uint32_t i = 0; i < block_size; ++i) {
            if (map_->test(key_maker_(rows->next()))) {
                bitmap->put(i);
            }
        }
        return bitmap;
    }

    namespace raw {
        template<typename DTYPE>
        Not<DTYPE>::Not(unique_ptr<lqf::RawAccessor<DTYPE>> inner):inner_(move(inner)) {}

        template<typename DTYPE>
        void Not<DTYPE>::init(uint64_t size) {
            inner_->init(size);
        }

        template<typename DTYPE>
        void Not<DTYPE>::dict(lqf::Dictionary<DTYPE> &dict) {
            inner_->dict(dict);
        }

        template<typename DTYPE>
        void Not<DTYPE>::data(DataPage *dpage) {
            inner_->data(dpage);
        }

        template<typename DTYPE>
        shared_ptr<Bitmap> Not<DTYPE>::result() {
            return ~(*inner_->result());
        }

        template<typename DTYPE>
        unique_ptr<RawAccessor<DTYPE>> Not<DTYPE>::build(function<unique_ptr<RawAccessor<DTYPE>>()> builder) {
            return unique_ptr<RawAccessor<DTYPE>>(new Not<DTYPE>(builder()));
        }

        template
        class Not<Int32Type>;

        template
        class Not<DoubleType>;

        template
        class Not<ByteArrayType>;
    }

    namespace sboost {

        template<typename DTYPE>
        shared_ptr<Bitmap> SboostPredicate<DTYPE>::filterBlock(Block &block, Bitmap &skip) {
//            unique_ptr<RawAccessor<DTYPE>> accessor = builder_();
//            return dynamic_cast<ParquetBlock &>(block).raw(index_, accessor.get());
            return skip & *FilterExecutor::inst->executeSboost(block, *this);
        }

        template<typename DTYPE>
        unique_ptr<RawAccessor<DTYPE>> SboostPredicate<DTYPE>::build() {
            return builder_();
        }

        template<typename DTYPE>
        DictEq<DTYPE>::DictEq(const T &target) : target_(target) {}

        template<typename DTYPE>
        void DictEq<DTYPE>::dict(Dictionary<DTYPE> &dict) {
            rawTarget_ = dict.lookup(target_);
        }

        template<typename DTYPE>
        void DictEq<DTYPE>::scanPage(uint64_t numEntry, const uint8_t *data,
                                     uint64_t *bitmap, uint64_t bitmap_offset) {
            uint8_t bitWidth = data[0];
            ::sboost::encoding::rlehybrid::equal(data + 1, bitmap, bitmap_offset, bitWidth,
                                                 numEntry, rawTarget_);
        }

        template<typename DTYPE>
        unique_ptr<DictEq<DTYPE>> DictEq<DTYPE>::build(const T &target) {
            return unique_ptr<DictEq<DTYPE>>(new DictEq<DTYPE>(target));
        }

        template<typename DTYPE>
        DictLess<DTYPE>::DictLess(const T &target) : target_(target) {}

        template<typename DTYPE>
        void DictLess<DTYPE>::dict(Dictionary<DTYPE> &dict) {
            rawTarget_ = dict.lookup(target_);
        };

        template<typename DTYPE>
        void DictLess<DTYPE>::scanPage(uint64_t numEntry, const uint8_t *data,
                                       uint64_t *bitmap, uint64_t bitmap_offset) {
            uint8_t bitWidth = data[0];
            ::sboost::encoding::rlehybrid::less(data + 1, bitmap, bitmap_offset, bitWidth,
                                                numEntry, rawTarget_);
        }

        template<typename DTYPE>
        unique_ptr<DictLess<DTYPE>> DictLess<DTYPE>::build(const T &target) {
            return unique_ptr<DictLess<DTYPE>>(new DictLess<DTYPE>(target));
        }

        template<typename DTYPE>
        DictGreater<DTYPE>::DictGreater(const T &target) : target_(target) {}

        template<typename DTYPE>
        void DictGreater<DTYPE>::dict(Dictionary<DTYPE> &dict) {
            rawTarget_ = dict.lookup(target_);
        };

        template<typename DTYPE>
        void DictGreater<DTYPE>::scanPage(uint64_t numEntry, const uint8_t *data,
                                          uint64_t *bitmap, uint64_t bitmap_offset) {
            uint8_t bitWidth = data[0];
            ::sboost::encoding::rlehybrid::greater(data + 1, bitmap, bitmap_offset, bitWidth,
                                                   numEntry, rawTarget_);
        }

        template<typename DTYPE>
        unique_ptr<DictGreater<DTYPE>> DictGreater<DTYPE>::build(const T &target) {
            return unique_ptr<DictGreater<DTYPE>>(new DictGreater<DTYPE>(target));
        }


        template<typename DTYPE>
        DictBetween<DTYPE>::DictBetween(const T &lower, const T &upper)
                : lower_(lower), upper_(upper) {}

        template<typename DTYPE>
        void DictBetween<DTYPE>::dict(Dictionary<DTYPE> &dict) {
            rawLower_ = dict.lookup(lower_);
            rawUpper_ = dict.lookup(upper_);
        };

        template<typename DTYPE>
        void DictBetween<DTYPE>::scanPage(uint64_t numEntry, const uint8_t *data,
                                          uint64_t *bitmap, uint64_t bitmap_offset) {
            uint8_t bitWidth = data[0];
            ::sboost::encoding::rlehybrid::between(data + 1, bitmap, bitmap_offset, bitWidth,
                                                   numEntry, rawLower_, rawUpper_);
        }

        template<typename DTYPE>
        unique_ptr<DictBetween<DTYPE>> DictBetween<DTYPE>::build(const T &lower, const T &upper) {
            return unique_ptr<DictBetween<DTYPE>>(new DictBetween<DTYPE>(lower, upper));
        }

        template<typename DTYPE>
        DictRangele<DTYPE>::DictRangele(const T &lower, const T &upper)
                : lower_(lower), upper_(upper) {}

        template<typename DTYPE>
        void DictRangele<DTYPE>::dict(Dictionary<DTYPE> &dict) {
            rawLower_ = dict.lookup(lower_);
            rawUpper_ = dict.lookup(upper_);
        };

        template<typename DTYPE>
        void DictRangele<DTYPE>::scanPage(uint64_t numEntry, const uint8_t *data,
                                          uint64_t *bitmap, uint64_t bitmap_offset) {
            uint8_t bitWidth = data[0];
            ::sboost::encoding::rlehybrid::rangele(data + 1, bitmap, bitmap_offset, bitWidth,
                                                   numEntry, rawLower_, rawUpper_);
        }

        template<typename DTYPE>
        unique_ptr<DictRangele<DTYPE>> DictRangele<DTYPE>::build(const T &lower, const T &upper) {
            return unique_ptr<DictRangele<DTYPE>>(new DictRangele<DTYPE>(lower, upper));
        }


        template<typename DTYPE>
        DictMultiEq<DTYPE>::DictMultiEq(function<bool(const T &)> pred) : predicate_(pred) {}

        template<typename DTYPE>
        void DictMultiEq<DTYPE>::dict(Dictionary<DTYPE> &dict) {
            keys_ = move(dict.list(predicate_));
        };

        template<typename DTYPE>
        void DictMultiEq<DTYPE>::scanPage(uint64_t numEntry, const uint8_t *data,
                                          uint64_t *bitmap, uint64_t bitmap_offset) {
            uint8_t bitWidth = data[0];
            // See SimpleBitmap for the reason of +2 here
            uint32_t buffer_size = (numEntry >> 6) + 2;
            uint64_t *page_oneresult = (uint64_t *) aligned_alloc(64, sizeof(uint64_t) * buffer_size);
            uint64_t *page_result = (uint64_t *) aligned_alloc(64, sizeof(uint64_t) * buffer_size);

            memset((void *) page_result, 0, sizeof(uint64_t) * buffer_size);

            auto ite = keys_.get()->begin();
            ::sboost::encoding::rlehybrid::equal(data + 1, page_result, 0, bitWidth,
                                                 numEntry, *ite);
            ite++;
            while (ite != keys_.get()->end()) {
                memset((void *) page_oneresult, 0, sizeof(uint64_t) * buffer_size);
                ::sboost::encoding::rlehybrid::equal(data + 1, page_oneresult, 0, bitWidth,
                                                     numEntry, *ite);
                ::sboost::simd::simd_or(page_result, page_oneresult, buffer_size);
                ite++;
            }
            ::sboost::BitmapWriter writer(bitmap, bitmap_offset);
            writer.appendWord(page_result, numEntry);
            free(page_oneresult);
            free(page_result);
        }

        template<typename DTYPE>
        unique_ptr<DictMultiEq<DTYPE>> DictMultiEq<DTYPE>::build(function<bool(const T &)> pred) {
            return unique_ptr<DictMultiEq<DTYPE>>(new DictMultiEq<DTYPE>(pred));
        }

        DeltaEq::DeltaEq(const int target) : target_(target) {}

        void DeltaEq::dict(Int32Dictionary &) {}

        void DeltaEq::scanPage(uint64_t numEntry, const uint8_t *data,
                               uint64_t *bitmap, uint64_t bitmap_offset) {
            ::sboost::encoding::deltabp::equal(data, bitmap, bitmap_offset, numEntry, target_);
        }

        unique_ptr<DeltaEq> DeltaEq::build(const int target) {
            return unique_ptr<DeltaEq>(new DeltaEq(target));
        }

        DeltaLess::DeltaLess(const int target) : target_(target) {}

        void DeltaLess::dict(Int32Dictionary &) {}

        void DeltaLess::scanPage(uint64_t numEntry, const uint8_t *data,
                                 uint64_t *bitmap, uint64_t bitmap_offset) {
            ::sboost::encoding::deltabp::less(data, bitmap, bitmap_offset, numEntry, target_);
        }

        unique_ptr<DeltaLess> DeltaLess::build(const int target) {
            return unique_ptr<DeltaLess>(new DeltaLess(target));
        }

        DeltaBetween::DeltaBetween(const int lower, const int upper)
                : lower_(lower), upper_(upper) {}

        void DeltaBetween::dict(Int32Dictionary &) {}

        void DeltaBetween::scanPage(uint64_t numEntry, const uint8_t *data,
                                    uint64_t *bitmap, uint64_t bitmap_offset) {
            ::sboost::encoding::deltabp::between(data, bitmap, bitmap_offset, numEntry, lower_, upper_);
        }

        unique_ptr<DeltaBetween> DeltaBetween::build(const int lower, const int upper) {
            return unique_ptr<DeltaBetween>(new DeltaBetween(lower, upper));
        }

        template
        class DictEq<Int32Type>;

        template
        class DictEq<DoubleType>;

        template
        class DictEq<ByteArrayType>;

        template
        class DictLess<Int32Type>;

        template
        class DictLess<DoubleType>;

        template
        class DictLess<ByteArrayType>;

        template
        class DictGreater<Int32Type>;

        template
        class DictGreater<DoubleType>;

        template
        class DictGreater<ByteArrayType>;

        template
        class DictBetween<Int32Type>;

        template
        class DictBetween<DoubleType>;

        template
        class DictBetween<ByteArrayType>;

        template
        class DictRangele<Int32Type>;

        template
        class DictRangele<DoubleType>;

        template
        class DictRangele<ByteArrayType>;

        template
        class DictMultiEq<Int32Type>;

        template
        class DictMultiEq<DoubleType>;

        template
        class DictMultiEq<ByteArrayType>;

        template
        class SboostPredicate<Int32Type>;

        template
        class SboostPredicate<DoubleType>;

        template
        class SboostPredicate<ByteArrayType>;
    }
}


