//
// Created by Harper on 4/29/20.
//

#ifndef ARROW_HASH_CONTAINER_H
#define ARROW_HASH_CONTAINER_H

#include "container.h"
#include "data_model.h"
#include "rowcopy.h"
#include "data_container.h"

#define CONTAINER_SIZE 1048576

namespace lqf {

    using namespace container;
    using namespace rowcopy;

    namespace hashcontainer {

        template<typename DTYPE>
        class IntPredicate {
            using ktype = typename DTYPE::type;
        public:
            virtual ~IntPredicate() = default;

            virtual bool test(ktype) = 0;
        };

        using Int32Predicate = IntPredicate<Int32>;
        using Int64Predicate = IntPredicate<Int64>;

        template<typename DTYPE>
        class HashPredicate : public IntPredicate<DTYPE> {
            using ktype = typename DTYPE::type;
        private:
            PhaseConcurrentHashSet<DTYPE> content_;
            atomic<ktype> min_;
            atomic<ktype> max_;
        public:
            HashPredicate();

            HashPredicate(uint32_t size);

            virtual ~HashPredicate() = default;

            void add(ktype);

            bool test(ktype) override;

            inline uint32_t size() { return content_.size(); }

            inline ktype max() { return max_.load(); }

            inline ktype min() { return min_.load(); }
        };

        using Hash32Predicate = HashPredicate<Int32>;
        using Hash64Predicate = HashPredicate<Int64>;

        class BitmapPredicate : public Int32Predicate {
        private:
            ConcurrentBitmap bitmap_;
        public:
            BitmapPredicate(uint32_t max);

            virtual ~BitmapPredicate() = default;

            void add(int32_t);

            bool test(int32_t) override;
        };

        using namespace datacontainer;

        template<typename DTYPE>
        class HashSparseContainer : public IntPredicate<DTYPE> {
            using ktype = typename DTYPE::type;
        protected:
            vector<uint32_t> col_offset_;
            PhaseConcurrentHashMap<DTYPE, MemDataRow *> map_;
            atomic<ktype> min_;
            atomic<ktype> max_;

        public:
            HashSparseContainer(const vector<uint32_t> &);

            HashSparseContainer(const vector<uint32_t> &, uint32_t size);

            virtual ~HashSparseContainer() = default;

            DataRow &add(ktype key);

            bool test(ktype) override;

            DataRow *get(ktype key);

            unique_ptr<DataRow> remove(ktype key);

            unique_ptr<lqf::Iterator<std::pair<ktype, DataRow &> &>> iterator();

            inline uint32_t size() { return map_.size(); }

            inline ktype min() { return min_.load(); }

            inline ktype max() { return max_.load(); }
        };

        template<typename DTYPE, typename MAP>
        class HashDenseContainer : public IntPredicate<DTYPE> {
            using ktype = typename DTYPE::type;
        protected:
            MAP map_;
            atomic<ktype> min_;
            atomic<ktype> max_;

        public:
            HashDenseContainer(const vector<uint32_t> &);

            HashDenseContainer(const vector<uint32_t> &, uint32_t size);

            virtual ~HashDenseContainer() = default;

            DataRow &add(ktype key);

            bool test(ktype) override;

            DataRow *get(ktype key);

            DataRow *remove(ktype key);

            unique_ptr<lqf::Iterator<std::pair<ktype, DataRow &> &>> iterator();

            inline uint32_t size() { return map_.size(); }

            inline ktype min() { return min_.load(); }

            inline ktype max() { return max_.load(); }
        };

        using Hash32SparseContainer = HashSparseContainer<Int32>;
        using Hash64SparseContainer = HashSparseContainer<Int64>;
        using Hash32DenseContainer = HashDenseContainer<Int32, CInt32MemRowMap>;
        using Hash64DenseContainer = HashDenseContainer<Int64, CInt64MemRowMap>;

        using Hash32Container = Hash32DenseContainer;
        using Hash64Container = Hash64DenseContainer;

        using namespace datacontainer;

        template<typename CONTENT>
        class HashMemBlock : public MemBlock {
        private:
            shared_ptr<CONTENT> content_;
        public:
            HashMemBlock(shared_ptr<CONTENT> predicate);

            virtual ~HashMemBlock() = default;

            shared_ptr<CONTENT> content();
        };

        class HashBuilder {
        public:
            static shared_ptr<Int32Predicate>
            buildHashPredicate(Table &input, uint32_t, uint32_t expect_size = CONTAINER_SIZE);

            static shared_ptr<Int64Predicate> buildHashPredicate(Table &input, function<int64_t(DataRow &)>);

            static shared_ptr<Int32Predicate> buildBitmapPredicate(Table &input, uint32_t, uint32_t);

//            static shared_ptr<Hash32NewContainer>
//            buildNewContainer(Table &input, uint32_t, Snapshoter *, uint32_t expect_size = CONTAINER_SIZE);
//
//            static shared_ptr<Hash64NewContainer>
//            buildNewContainer(Table &input, function<int64_t(DataRow &)>, Snapshoter *,
//                           uint32_t expect_size = CONTAINER_SIZE);

            static shared_ptr<Hash32Container>
            buildContainer(Table &input, uint32_t, Snapshoter *, uint32_t expect_size = CONTAINER_SIZE);

            static shared_ptr<Hash64Container>
            buildContainer(Table &input, function<int64_t(DataRow &)>, Snapshoter *,
                           uint32_t expect_size = CONTAINER_SIZE);
        };

    }
}


#endif //ARROW_HASH_CONTAINER_H
