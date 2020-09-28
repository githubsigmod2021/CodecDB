//
// Created by harper on 2/21/20.
//

#ifndef LQF_OPERATOR_FILTER_H
#define LQF_OPERATOR_FILTER_H

#include <memory>
#include "lang.h"
#include "parallel.h"
#include "data_model.h"
#include "bitmap.h"
#include "hash_container.h"
#include <sboost/encoding/rlehybrid.h>
#include <sboost/encoding/deltabp.h>

#define ACCESS(type, param)
#define ACCESS2(type, param1, param2)

using namespace std;

namespace lqf {

    using namespace parallel;

    class Filter : public Node {
    protected:
        shared_ptr<Block> processBlock(const shared_ptr<Block> &);

        virtual shared_ptr<Bitmap> filterBlock(Block &) = 0;

    public:
        Filter();

        virtual ~Filter() = default;

        virtual shared_ptr<Table> filter(Table &input);

        unique_ptr<NodeOutput> execute(const vector<NodeOutput *> &) override;
    };

    class ColPredicate {
    protected:
        uint32_t index_;
    public:
        ColPredicate(uint32_t);

        virtual ~ColPredicate() = default;

        inline uint32_t index() { return index_; }

        virtual shared_ptr<Bitmap> filterBlock(Block &, Bitmap &) = 0;
    };

    class SimplePredicate : public ColPredicate {
    private:
        function<bool(const DataField &)> predicate_;
    public:
        SimplePredicate(uint32_t, function<bool(const DataField &)>);

        ~SimplePredicate() = default;

        void predicate(function<bool(const DataField &)>);

        shared_ptr<Bitmap> filterBlock(Block &, Bitmap &) override;
    };

    namespace raw {

        template<typename DTYPE>
        class Not : RawAccessor<DTYPE> {
        protected:
            unique_ptr<RawAccessor<DTYPE>> inner_;
        public:
            Not(unique_ptr<RawAccessor<DTYPE>> inner);

            virtual ~Not() = default;

            void init(uint64_t) override;

            void dict(Dictionary<DTYPE> &dict) override;

            void data(DataPage *dpage) override;

            shared_ptr<Bitmap> result() override;

            static unique_ptr<RawAccessor<DTYPE>> build(function<unique_ptr<RawAccessor<DTYPE>>()>);
        };

        using Int32Not = Not<Int32Type>;
        using DoubleNot = Not<DoubleType>;
        using ByteArrayNot = Not<ByteArrayType>;
    }

    namespace sboost {

        template<typename DTYPE>
        class SboostPredicate : public ColPredicate {
        public:
            SboostPredicate(uint32_t index, function<unique_ptr<RawAccessor<DTYPE>>()> accbuilder)
                    : ColPredicate(index), builder_(accbuilder) {}

            virtual ~SboostPredicate() = default;

            shared_ptr<Bitmap> filterBlock(Block &block, Bitmap &) override;

            unique_ptr<RawAccessor<DTYPE>> build();

        protected:
            function<unique_ptr<RawAccessor<DTYPE>>()> builder_;
        };

        using SBoostInt32Predicate = SboostPredicate<Int32Type>;
        using SBoostDoublePredicate = SboostPredicate<DoubleType>;
        using SBoostByteArrayPredicate = SboostPredicate<ByteArrayType>;

        template<typename DTYPE>
        class DictEq : public RawAccessor<DTYPE> {
            using T = typename DTYPE::c_type;
        private:
            const T &target_;
            int rawTarget_;
        public:
            DictEq(const T &target);

            virtual ~DictEq() = default;

            void dict(Dictionary<DTYPE> &dict) override;

            void scanPage(uint64_t numEntry, const uint8_t *data,
                          uint64_t *bitmap, uint64_t bitmap_offset) override;

            static unique_ptr<DictEq<DTYPE>> build(const T &target);
        };

        using Int32DictEq = DictEq<Int32Type>;
        using DoubleDictEq = DictEq<DoubleType>;
        using ByteArrayDictEq = DictEq<ByteArrayType>;

        template<typename DTYPE>
        class DictLess : public RawAccessor<DTYPE> {
            using T = typename DTYPE::c_type;
            const T &target_;
            int rawTarget_;
        public:
            DictLess(const T &target);

            virtual ~DictLess() = default;

            void dict(Dictionary<DTYPE> &dict) override;

            void scanPage(uint64_t numEntry, const uint8_t *data,
                          uint64_t *bitmap, uint64_t bitmap_offset) override;

            static unique_ptr<DictLess<DTYPE>> build(const T &target);
        };

        using Int32DictLess = DictLess<Int32Type>;
        using DoubleDictLess = DictLess<DoubleType>;
        using ByteArrayDictLess = DictLess<ByteArrayType>;

        template<typename DTYPE>
        class DictGreater : public RawAccessor<DTYPE> {
            using T = typename DTYPE::c_type;
            const T &target_;
            int rawTarget_;
        public:
            DictGreater(const T &target);

            virtual ~DictGreater() = default;

            void dict(Dictionary<DTYPE> &dict) override;

            void scanPage(uint64_t numEntry, const uint8_t *data,
                          uint64_t *bitmap, uint64_t bitmap_offset) override;

            static unique_ptr<DictGreater<DTYPE>> build(const T &target);
        };

        using Int32DictGreater = DictGreater<Int32Type>;
        using DoubleDictGreater = DictGreater<DoubleType>;
        using ByteArrayDictGreater = DictGreater<ByteArrayType>;

        template<typename DTYPE>
        class DictBetween : public RawAccessor<DTYPE> {
            using T = typename DTYPE::c_type;
            const T &lower_;
            const T &upper_;
            int rawLower_;
            int rawUpper_;
        public:
            DictBetween(const T &lower, const T &upper);

            virtual ~DictBetween() = default;

            void dict(Dictionary<DTYPE> &dict) override;

            void scanPage(uint64_t numEntry, const uint8_t *data,
                          uint64_t *bitmap, uint64_t bitmap_offset) override;

            static unique_ptr<DictBetween<DTYPE>> build(const T &lower, const T &upper);
        };

        using Int32DictBetween = DictBetween<Int32Type>;
        using DoubleDictBetween = DictBetween<DoubleType>;
        using ByteArrayDictBetween = DictBetween<ByteArrayType>;

        template<typename DTYPE>
        class DictRangele : public RawAccessor<DTYPE> {
            using T = typename DTYPE::c_type;
            const T &lower_;
            const T &upper_;
            int rawLower_;
            int rawUpper_;
        public:
            DictRangele(const T &lower, const T &upper);

            virtual ~DictRangele() = default;

            void dict(Dictionary<DTYPE> &dict) override;

            void scanPage(uint64_t numEntry, const uint8_t *data,
                          uint64_t *bitmap, uint64_t bitmap_offset) override;

            static unique_ptr<DictRangele<DTYPE>> build(const T &lower, const T &upper);
        };

        using Int32DictRangele = DictRangele<Int32Type>;
        using DoubleDictRangele = DictRangele<DoubleType>;
        using ByteArrayDictRangele = DictRangele<ByteArrayType>;

        template<typename DTYPE>
        class DictMultiEq : public RawAccessor<DTYPE> {
            using T = typename DTYPE::c_type;
            function<bool(const T &)> predicate_;
            unique_ptr<vector<uint32_t>> keys_;
        public:
            DictMultiEq(function<bool(const T &)> pred);

            virtual ~DictMultiEq() = default;

            void dict(Dictionary<DTYPE> &dict) override;

            void scanPage(uint64_t numEntry, const uint8_t *data,
                          uint64_t *bitmap, uint64_t bitmap_offset) override;

            static unique_ptr<DictMultiEq<DTYPE>> build(function<bool(const T &)>);
        };

        using Int32DictMultiEq = DictMultiEq<Int32Type>;
        using DoubleDictMultiEq = DictMultiEq<DoubleType>;
        using ByteArrayDictMultiEq = DictMultiEq<ByteArrayType>;

        class DeltaEq : public RawAccessor<Int32Type> {
        private:
            const int target_;
        public:
            DeltaEq(const int target);

            virtual ~DeltaEq() = default;

            void dict(Int32Dictionary &) override;

            void scanPage(uint64_t numEntry, const uint8_t *data,
                          uint64_t *bitmap, uint64_t bitmap_offset) override;

            static unique_ptr<DeltaEq> build(const int target);
        };

        class DeltaLess : public RawAccessor<Int32Type> {
        private:
            const int target_;
        public:
            DeltaLess(const int target);

            virtual ~DeltaLess() = default;

            void dict(Int32Dictionary &) override;

            void scanPage(uint64_t numEntry, const uint8_t *data,
                          uint64_t *bitmap, uint64_t bitmap_offset) override;

            static unique_ptr<DeltaLess> build(const int target);
        };

        class DeltaBetween : public RawAccessor<Int32Type> {
        private:
            const int lower_;
            const int upper_;
        public:
            DeltaBetween(const int lower, const int upper);

            virtual ~DeltaBetween() = default;

            void dict(Int32Dictionary &) override;

            void scanPage(uint64_t numEntry, const uint8_t *data,
                          uint64_t *bitmap, uint64_t bitmap_offset) override;

            static unique_ptr<DeltaBetween> build(const int lower, const int upper);
        };
    }

    class ColFilter : public Filter {
    protected:
        vector<unique_ptr<ColPredicate>> predicates_;

        virtual shared_ptr<Bitmap> filterBlock(Block &input) override;

    public:
        ColFilter(ColPredicate *);

        ColFilter(initializer_list<ColPredicate *>);

        virtual ~ColFilter();

        ColPredicate *predicate(uint32_t);

        shared_ptr<Table> filter(Table &input) override;
    };

    class RowFilter : public Filter {
    private:
        function<bool(DataRow &)> predicate_;

        virtual shared_ptr<Bitmap> filterBlock(Block &input) override;

    public:
        RowFilter(function<bool(DataRow &)> pred);

        virtual ~RowFilter() = default;
    };

    /**
     * Filter the data on column1 < column2
     * The columns must use the same dictionary
     */
    class SboostRowFilter : public Filter {
    protected:
        uint32_t column1_;

        uint32_t column2_;

        virtual shared_ptr<Bitmap> filterBlock(Block &) override;

    public:
        SboostRowFilter(uint32_t, uint32_t);

        virtual ~SboostRowFilter() = default;
    };

    class SboostRow2Filter : public Filter {
    protected:
        uint32_t column1_;
        uint32_t column2_;
        uint32_t column3_;

        virtual shared_ptr<Bitmap> filterBlock(Block &) override;

    public:
        SboostRow2Filter(uint32_t, uint32_t, uint32_t);

        virtual ~SboostRow2Filter() = default;
    };

    using namespace hashcontainer;

    class MapFilter : public Filter {
    private:
        uint32_t key_index_;
        IntPredicate<Int32> *map_;
    public:
        MapFilter(uint32_t);

        MapFilter(uint32_t key_index, IntPredicate<Int32> &);

        virtual ~MapFilter() = default;

        void setMap(IntPredicate<Int32> &);

        virtual shared_ptr<Bitmap> filterBlock(Block &input) override;
    };

    class PowerMapFilter : public Filter {
    private:
        function<uint64_t(DataRow &)> key_maker_;
        IntPredicate<Int64> *map_;
    public:
        PowerMapFilter(function<uint64_t(DataRow &)>);

        PowerMapFilter(function<uint64_t(DataRow &)>, IntPredicate<Int64> &);

        virtual ~PowerMapFilter() = default;

        void setMap(IntPredicate<Int64> &);

        virtual shared_ptr<Bitmap> filterBlock(Block &input) override;
    };

    class KeyFinder {
    protected:
        uint32_t key_index_;
        function<bool(DataRow &)> predicate_;

        int32_t filterBlock(const shared_ptr<Block> &);

    public:
        KeyFinder(uint32_t key_index, function<bool(DataRow &)>);

        virtual ~KeyFinder() = default;

        int32_t find(Table &);
    };
}
#endif //LQF_OPERATOR_FILTER_H
