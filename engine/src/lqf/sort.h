//
// Created by harper on 2/27/20.
//

#ifndef ARROW_SORT_H
#define ARROW_SORT_H

#include "heap.h"
#include "data_model.h"
#include "parallel.h"

#define SORTER1(x) [](DataRow *a, DataRow *b) {return (*a)[x].asInt() < (*b)[x].asInt();}
#define SORTER2(x, y) [](DataRow *a, DataRow *b) { auto a0 = (*a)[x].asInt(); auto b0 = (*b)[x].asInt(); return a0 < b0 || (a0 == b0 && (*a)[y].asInt() < (*b)[y].asInt()); }
#define SDGE(x) (*a)[x].asDouble() > (*b)[x].asDouble()
#define SDLE(x) (*a)[x].asDouble() < (*b)[x].asDouble()
#define SDE(x) (*a)[x].asDouble() == (*b)[x].asDouble()
#define SBLE(x) (*a)[x].asByteArray() < (*b)[x].asByteArray()
#define SBE(x) (*a)[x].asByteArray() == (*b)[x].asByteArray()
#define SILE(x) (*a)[x].asInt() < (*b)[x].asInt()
#define SIGE(x) (*a)[x].asInt() > (*b)[x].asInt()
#define SIE(x) (*a)[x].asInt() == (*b)[x].asInt()
using namespace std;

namespace lqf {


    using namespace parallel;

    namespace sort {
        class SortBlock : public Block {
        private:
            vector<DataRow *> content_;
            vector<shared_ptr<Block>> sources_;
        public:
            SortBlock();

            SortBlock(SortBlock &) = delete;

            SortBlock(SortBlock &&) = delete;

            virtual ~SortBlock();

            SortBlock &operator=(SortBlock &) = delete;

            SortBlock &operator=(SortBlock &&) = delete;

            uint64_t size() override;

            void copy(const shared_ptr<Block> &source);

            inline vector<DataRow *> &content() { return content_; }

            unique_ptr<ColumnIterator> col(uint32_t) override;

            unique_ptr<DataRowIterator> rows() override;

            shared_ptr<Block> mask(shared_ptr<Bitmap>) override;
        };
    }
    /**
     *
     */
    class SmallSort : public Node {
    protected:
        function<bool(DataRow *, DataRow *)> comparator_;
        bool vertical_ = false;

    public:
        SmallSort(function<bool(DataRow *, DataRow *)>, bool vertical = false);

        virtual ~SmallSort() = default;

        unique_ptr<NodeOutput> execute(const vector<NodeOutput *> &) override;

        shared_ptr<Table> sort(Table &);
    };

    class SnapshotSort : public Node {
    protected:
        vector<uint32_t> col_size_;
        function<bool(DataRow *, DataRow *)> comparator_;
        function<unique_ptr<MemDataRow>(DataRow &)> snapshoter_;
        bool vertical_;
    public:
        SnapshotSort(const vector<uint32_t>, function<bool(DataRow *, DataRow *)>,
                     function<unique_ptr<MemDataRow>(DataRow &)>, bool vertical = false);

        virtual ~SnapshotSort() = default;

        unique_ptr<NodeOutput> execute(const vector<NodeOutput *> &) override;

        shared_ptr<Table> sort(Table &);
    };

    class TopN : public Node {
    private:
        uint32_t n_;
        mutex collector_lock_;
        function<bool(DataRow *, DataRow *)> comparator_;
        bool vertical_ = false;
    public:
        TopN(uint32_t, function<bool(DataRow *, DataRow *)>, bool vertical = false);

        virtual ~TopN() = default;

        unique_ptr<NodeOutput> execute(const vector<NodeOutput *> &) override;

        shared_ptr<Table> sort(Table &);

    protected:
        void sortBlock(vector<DataRow *> *, MemTable *, const shared_ptr<Block> &input);
    };
}
#endif //ARROW_SORT_H
