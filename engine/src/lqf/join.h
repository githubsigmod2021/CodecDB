//
// Created by harper on 2/25/20.
//

#ifndef CHIDATA_LQF_JOIN_H
#define CHIDATA_LQF_JOIN_H

#include <climits>
#include "data_model.h"
#include "hash_container.h"
#include "container.h"
#include "rowcopy.h"

#define JL(x) x
#define JR(x) x | 0x10000
#define JLS(x) x | 0x20000
#define JRS(x) x | 0x30000
#define JLR(x) x | 0x40000
#define JRR(x) x | 0x50000

#define COL_HASHER(x) [](DataRow& input) { return input[x].asInt(); }
#define COL_HASHER2(x, y) [](DataRow& input) { return (static_cast<int64_t>(input[x].asInt()) << 32) + input[y].asInt(); }

namespace lqf {
    using namespace std;
    using namespace lqf::container;
    using namespace lqf::rowcopy;

    static function<bool(DataRow &, DataRow &)> TRUE = [](DataRow &a, DataRow &b) { return true; };

    namespace join {

        class JoinBuilder {
        protected:
            bool needkey_;
            bool vertical_;

            vector<int32_t> field_list_;

            TABLE_TYPE left_type_;
            TABLE_TYPE right_type_;

            vector<uint32_t> left_col_offset_;
            vector<uint32_t> right_col_offset_;

            vector<uint32_t> output_col_offset_;
            vector<uint32_t> output_col_size_;

            vector<uint32_t> snapshot_col_offset_;
            vector<uint32_t> snapshot_col_size_;

            unique_ptr<Snapshoter> snapshoter_;

        public:
            JoinBuilder(initializer_list<int32_t>, bool needkey, bool vertical);

            virtual ~JoinBuilder() = default;

            void on(Table &, Table &);

            virtual void init();

            inline bool useVertical() { return vertical_; }

            inline vector<uint32_t> &outputColSize() { return output_col_size_; }

            inline vector<uint32_t> &outputColOffset() { return output_col_offset_; }

            inline Snapshoter *snapshoter() { return snapshoter_.get(); }
        };

        class RowBuilder : public JoinBuilder {
        protected:
            unique_ptr<function<void(DataRow &, DataRow &)>> left_copier_;
            unique_ptr<function<void(DataRow &, DataRow &)>> right_copier_;

        public:
            RowBuilder(initializer_list<int32_t>, bool needkey = false, bool vertical = false);

            virtual ~RowBuilder() = default;

            virtual void init() override;

            virtual void build(DataRow &, DataRow &, DataRow &, int32_t key);
        };

        /// For use with VJoin
        class ColumnBuilder : public JoinBuilder {
        protected:
            vector<pair<uint8_t, uint8_t>> left_merge_inst_;
            vector<pair<uint8_t, uint8_t>> right_merge_inst_;
        public:
            ColumnBuilder(initializer_list<int32_t>);

            virtual ~ColumnBuilder() = default;

            virtual void init() override;

            inline const vector<uint32_t> &rightColSize() { return snapshot_col_size_; }

            virtual void build(MemvBlock &, MemvBlock &, MemvBlock &);
        };

    }

    using namespace join;
    using namespace hashcontainer;
    using namespace parallel;

    class Join : public Node {
    public:
        Join();

        virtual ~Join() = default;

        unique_ptr<NodeOutput> execute(const vector<NodeOutput *> &) override;

        virtual shared_ptr<Table> join(Table &, Table &) = 0;
    };

    class HashBasedJoin : public Join {
    protected:
        uint32_t leftKeyIndex_;
        uint32_t rightKeyIndex_;
        unique_ptr<JoinBuilder> builder_;
        shared_ptr<Hash32Container> container_;
        bool outer_ = false;
        uint32_t expect_size_;
    public:
        HashBasedJoin(uint32_t leftKeyIndex, uint32_t rightKeyIndex,
                      JoinBuilder *builder, uint32_t expect_size = CONTAINER_SIZE);

        virtual ~HashBasedJoin() = default;

        virtual shared_ptr<Table> join(Table &left, Table &right) override;

        inline void useOuter() { outer_ = true; };
    protected:
        virtual shared_ptr<Block> probe(const shared_ptr<Block> &leftBlock) = 0;

        shared_ptr<Block> makeBlock(uint32_t);

        shared_ptr<TableView> makeTable(unique_ptr<Stream<shared_ptr<Block>>>);
    };

    class HashJoin : public HashBasedJoin {
    public:
        HashJoin(uint32_t, uint32_t, RowBuilder *, function<bool(DataRow &, DataRow &)> pred = nullptr,
                 uint32_t expect_size = CONTAINER_SIZE);

        virtual ~HashJoin() = default;

    protected:
        RowBuilder *rowBuilder_;
        function<bool(DataRow &, DataRow &)> predicate_;

        virtual shared_ptr<Block> probe(const shared_ptr<Block> &) override;
    };

    class FilterJoin : public Join {
    protected:
        uint32_t leftKeyIndex_;
        uint32_t rightKeyIndex_;
        uint32_t expect_size_;
        shared_ptr<Int32Predicate> predicate_;
        bool anti_ = false;
        bool useBitmap_;
    public:
        FilterJoin(uint32_t leftKeyIndex, uint32_t rightKeyIndex, uint32_t expect_size = CONTAINER_SIZE,
                   bool useBitmap = false);

        virtual ~FilterJoin() = default;

        virtual shared_ptr<Table> join(Table &left, Table &right) override;

        void useAnti() { anti_ = true; }

    protected:
        virtual shared_ptr<Block> probe(const shared_ptr<Block> &);
    };

    class FilterTransformJoin : public FilterJoin {
    protected:
        unique_ptr<Snapshoter> match_writer_;
        unique_ptr<Snapshoter> unmatch_writer_;

        shared_ptr<Block> probe(const shared_ptr<Block> &) override;

    public:
        FilterTransformJoin(uint32_t, uint32_t, unique_ptr<Snapshoter>, unique_ptr<Snapshoter>,
                            uint32_t expect_size = CONTAINER_SIZE,
                            bool use_bitmap = false);

        virtual ~FilterTransformJoin() = default;
    };

    /*
     * HashExistJoin works by creating a hash table containing all records to be checked,
     * and probe using another table. All records in the hashtable that has at least one match
     * will be output. It is different from HashFilterJoin in the case that the results are
     * output from the build table, not the probe table.
     *
     * When there's no predicate present, we remove a record from the hash table when one
     * match appears. This makes sure a record only appears in the output once.
     *
     * When there's a predicate, we fetch a record from the hash table, apply the predicate.
     * If the result is success, we mark the key in a bitmap. Keys already in the bitmap will
     * not be further checked. Bitmaps from different blocks will be logical or to obtain the final
     * result. The reason for this complicated operation is that with existence of a predicate,
     * we need to perform either a remove-check-putback or get-check-remove operation.
     * These operations are not parallelizable.
     */


    class HashExistJoin : public HashBasedJoin {

    public:
        HashExistJoin(uint32_t leftKeyIndex, uint32_t rightKeyIndex, JoinBuilder *rowBuilder,
                      function<bool(DataRow &, DataRow &)> pred = nullptr);

        virtual ~HashExistJoin() = default;

        shared_ptr<Table> join(Table &, Table &) override;

    protected:
        function<bool(DataRow &, DataRow &)> predicate_;

        shared_ptr<Block> probe(const shared_ptr<Block> &leftBlock) override;

        shared_ptr<SimpleBitmap> probeWithPredicate(const shared_ptr<Block> &leftBlock);
    };

    class HashNotExistJoin : public HashExistJoin {
    public:
        HashNotExistJoin(uint32_t leftKeyIndex, uint32_t rightKeyIndex, JoinBuilder *rowBuilder,
                         function<bool(DataRow &, DataRow &)> pred = nullptr);

        virtual ~HashNotExistJoin() = default;

        shared_ptr<Table> join(Table &, Table &) override;

    protected:

        void scan(const shared_ptr<Block> &);

    };

    /**
     * HashColumnJoin is used to perform joining on vertical memory table.
     * It allows reading only columns participating joining, and avoids
     * unnecessary data movement between memory tables when multiple
     * joins are performed in sequence.
     */
    class HashColumnJoin : public HashBasedJoin {
    public:
        HashColumnJoin(uint32_t, uint32_t, ColumnBuilder *, uint32_t expect_size = CONTAINER_SIZE);

        virtual ~HashColumnJoin() = default;

    protected:
        ColumnBuilder *columnBuilder_;

        shared_ptr<Block> probe(const shared_ptr<Block> &) override;
    };

    /**
     * Build a hash table on multiple entries with same key
     */
    class HashMultiJoin : public Join {
    protected:
        uint32_t left_key_index_;
        uint32_t right_key_index_;
        unique_ptr<RowBuilder> builder_;
        unordered_map<int32_t, unique_ptr<vector<unique_ptr<MemDataRow>>>> container_;

        void buildmap(const shared_ptr<Block> &);

        shared_ptr<Block> probe(const shared_ptr<Block> &);

    public:

        HashMultiJoin(uint32_t, uint32_t, RowBuilder *);

        shared_ptr<Table> join(Table &, Table &) override;
    };

    namespace powerjoin {

        class PowerHashBasedJoin : public Join {
        protected:
            function<int64_t(DataRow &)> left_key_maker_;
            function<int64_t(DataRow &)> right_key_maker_;
            unique_ptr<JoinBuilder> builder_;
            shared_ptr<Hash64Container> container_;
            function<bool(DataRow &, DataRow &)> predicate_;
            uint32_t expect_size_;
            bool outer_ = false;
        public:
            PowerHashBasedJoin(function<int64_t(DataRow &)>, function<int64_t(DataRow &)>,
                               JoinBuilder *, uint32_t expect_size = CONTAINER_SIZE,
                               function<bool(DataRow &, DataRow &)> pred = nullptr);

            virtual ~PowerHashBasedJoin() = default;

            virtual shared_ptr<Table> join(Table &left, Table &right) override;

            inline void useOuter() { outer_ = true; }

        protected:
            virtual void probe(MemTable *, const shared_ptr<Block> &) = 0;
        };

        class PowerHashJoin : public PowerHashBasedJoin {
        public:
            PowerHashJoin(function<int64_t(DataRow &)>, function<int64_t(DataRow &)>, RowBuilder *,
                          uint32_t expect_size = CONTAINER_SIZE, function<bool(DataRow &, DataRow &)> pred = nullptr);

            virtual ~PowerHashJoin() = default;

        protected:
            RowBuilder *rowBuilder_;

            void probe(MemTable *, const shared_ptr<Block> &) override;
        };

        class PowerHashColumnJoin : public PowerHashBasedJoin {
        public:
            PowerHashColumnJoin(function<int64_t(DataRow &)>, function<int64_t(DataRow &)>, ColumnBuilder *);

            virtual ~PowerHashColumnJoin() = default;

        protected:
            ColumnBuilder *columnBuilder_;

            void probe(MemTable *, const shared_ptr<Block> &) override;
        };

        class PowerHashFilterJoin : public Join {
        protected:
            function<int64_t(DataRow &)> left_key_maker_;
            function<int64_t(DataRow &)> right_key_maker_;
            shared_ptr<Int64Predicate> predicate_;
            bool anti_ = false;
        public:
            PowerHashFilterJoin(function<int64_t(DataRow &)>, function<int64_t(DataRow &)>);

            virtual ~PowerHashFilterJoin() = default;

            virtual shared_ptr<Table> join(Table &left, Table &right) override;

            void useAnti() { anti_ = true; }

        protected:
            shared_ptr<Block> probe(const shared_ptr<Block> &leftBlock);
        };
    }
}
#endif //CHIDATA_LQF_JOIN_H
