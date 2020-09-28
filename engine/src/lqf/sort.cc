//
// Created by harper on 2/27/20.
//

#include <memory>
#include "sort.h"

using namespace std;
using namespace std::placeholders;

namespace lqf {

    namespace sort {
        SortBlock::SortBlock() {}

        SortBlock::~SortBlock() {
            for (auto &item: content_) {
                delete item;
            }
        }

        uint64_t SortBlock::size() {
            return content_.size();
        }

        void SortBlock::copy(const shared_ptr<Block> &source) {
            sources_.push_back(source);
            auto rows = source->rows();
            auto size = source->size();
            for (uint32_t i = 0; i < size; ++i) {
                content_.push_back(rows->next().snapshot().release());
            }
        }

        class SortColumnIterator : public ColumnIterator {
        protected:
            vector<DataRow *> &ref_;
            uint32_t col_index_;
            uint32_t index_;
        public:
            SortColumnIterator(vector<DataRow *> &ref, uint32_t col_index)
                    : ref_(ref), col_index_(col_index), index_(-1) {}

            DataField &next() override {
                index_++;
                return (*ref_[index_])[col_index_];
            }

            uint64_t pos() override {
                return index_;
            }

            DataField &operator[](uint64_t index) override {
                index_ = index;
                return (*ref_[index_])[col_index_];
            }
        };

        unique_ptr<ColumnIterator> SortBlock::col(uint32_t col_index) {
            return unique_ptr<ColumnIterator>(new SortColumnIterator(content_, col_index));
        }

        class SortRowIterator : public DataRowIterator {
        protected:
            vector<DataRow *> &ref_;
            uint32_t index_;
        public:
            SortRowIterator(vector<DataRow *> &ref) : ref_(ref), index_(-1) {}

            DataRow &next() override {
                index_++;
                return *(ref_[index_]);
            }

            uint64_t pos() override {
                return index_;
            }

            DataRow &operator[](uint64_t index) override {
                index_ = index;
                return *(ref_[index_]);
            }
        };

        unique_ptr<DataRowIterator> SortBlock::rows() {
            return unique_ptr<DataRowIterator>(new SortRowIterator(content_));
        }

        shared_ptr<Block> SortBlock::mask(shared_ptr<Bitmap> mask) {
            return make_shared<MaskedBlock>(shared_from_this(), mask);
        }
    }
    using namespace sort;

    SmallSort::SmallSort(function<bool(DataRow *, DataRow *)> comp, bool vertical)
            : Node(1), comparator_(comp), vertical_(vertical) {}

    unique_ptr<NodeOutput> SmallSort::execute(const vector<NodeOutput *> &inputs) {
        auto input0 = static_cast<TableOutput *>(inputs[0]);
        auto table = sort(*(input0->get()));
        return unique_ptr<TableOutput>(new TableOutput(table));
    }

    shared_ptr<Table> SmallSort::sort(Table &table) {
        auto output = MemTable::Make(table.colSize(), false);

        auto sblock = make_shared<SortBlock>();
        // TODO Note the multi-thread here
        table.blocks()->sequential()->foreach([sblock](const shared_ptr<Block> &block) {
            sblock->copy(block);
        });
        auto &container = sblock->content();
        std::sort(container.begin(), container.end(), comparator_);
        output->append(sblock);
        return output;
    }

    SnapshotSort::SnapshotSort(const vector<uint32_t> col_size,
                               function<bool(DataRow *, DataRow *)> comp,
                               function<unique_ptr<MemDataRow>(DataRow &)> snapshoter, bool vertical)
            : Node(1), col_size_(col_size), comparator_(comp), snapshoter_(snapshoter), vertical_(vertical) {}

    unique_ptr<NodeOutput> SnapshotSort::execute(const vector<NodeOutput *> &inputs) {
        auto input0 = static_cast<TableOutput *>(inputs[0]);
        auto table = sort(*(input0->get()));
        return unique_ptr<TableOutput>(new TableOutput(table));
    }

    shared_ptr<Table> SnapshotSort::sort(Table &input) {
        auto memblock = make_shared<SortBlock>();
        auto row_cache = &(memblock->content());
        input.blocks()->sequential()->foreach([this, row_cache](const shared_ptr<Block> &block) {
            auto block_size = block->size();
            auto rows = block->rows();
            for (uint32_t i = 0; i < block_size; ++i) {
                row_cache->push_back(snapshoter_(rows->next()).release());
            }
        });

        std::sort(row_cache->begin(), row_cache->end(), comparator_);
        auto resultTable = MemTable::Make(col_size_, vertical_);
        resultTable->append(memblock);
        return resultTable;
    }

    TopN::TopN(uint32_t n, function<bool(DataRow *, DataRow *)> comp, bool vertical)
            : Node(1), n_(n), comparator_(comp), vertical_(vertical) {}

    unique_ptr<NodeOutput> TopN::execute(const vector<NodeOutput *> &inputs) {
        auto input0 = static_cast<TableOutput *>(inputs[0]);
        auto table = sort(*(input0->get()));
        return unique_ptr<TableOutput>(new TableOutput(table));
    }

    shared_ptr<Table> TopN::sort(Table &table) {
        auto resultTable = MemTable::Make(table.colSize(), vertical_);

        vector<DataRow *> collector;

        function<void(const shared_ptr<Block> &)> proc =
                bind(&TopN::sortBlock, this, &collector, resultTable.get(), _1);
        table.blocks()->foreach(proc);

        /// Make a final sort and fetch the top n
        if (collector.size() > n_) {
            std::sort(collector.begin(), collector.end(), comparator_);
        }

        auto result_size = std::min(n_, static_cast<uint32_t>(collector.size()));
        auto resultBlock = resultTable->allocate(result_size);
        auto resultRows = resultBlock->rows();

        for (uint32_t i = 0; i < result_size; ++i) {
            (*resultRows)[i] = *(collector[i]);
        }
        for (auto &item:collector) {
            delete item;
        }
        return resultTable;
    }

    void TopN::sortBlock(vector<DataRow *> *collector, MemTable *dest, const shared_ptr<Block> &input) {
        Heap<DataRow *> heap(n_, [=]() { return new MemDataRow(dest->colOffset()); }, comparator_);
        auto rows = input->rows();
        auto block_size = input->size();
        for (uint32_t i = 0; i < block_size; ++i) {
            DataRow &row = rows->next();
            heap.add(&row);
        }
        heap.done();

        /// Collect the result
        collector_lock_.lock();
        auto content = heap.content();
        collector->insert(collector->end(), content.begin(), content.end());
        // So heap will not delete these pointers as they have been moved
        heap.content().clear();
        collector_lock_.unlock();
    }
}
