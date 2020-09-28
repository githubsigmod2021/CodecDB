//
// Created by Harper on 6/15/20.
//

#ifndef ARROW_DATA_CONTAINER_H
#define ARROW_DATA_CONTAINER_H

#include "lang.h"
#include "data_model.h"
#include "container.h"
#include "concurrent.h"

namespace lqf {
    namespace datacontainer {
// The buffer size actually affect running speed.
#define VECTOR_SLAB_SIZE_ 131072

        class MemRowVector {
        protected:
            MemDataRowPointer accessor_;
            vector<shared_ptr<vector<uint64_t>>> memory_;
            uint32_t slab_size_;
            uint32_t stripe_offset_;
            uint32_t size_;
            uint32_t row_size_ = 0;
            uint32_t stripe_size_ = 0;
        public:
            MemRowVector(const vector<uint32_t> &offset);

            MemRowVector(const vector<uint32_t> &offset, uint32_t slab_size);

            DataRow &push_back();

            DataRow &operator[](uint32_t index);

            inline uint32_t size() { return size_; }

            inline uint32_t slab_size() { return slab_size_; }
            inline vector<shared_ptr<vector<uint64_t>>> &memory() { return memory_; }

            unique_ptr<Iterator<DataRow &>> iterator();
        };

        class MemRowMap : public MemRowVector {
        protected:
            unordered_map<uint64_t, uint64_t> map_;
        public:
            MemRowMap(const vector<uint32_t> &offset);

            MemRowMap(const vector<uint32_t> &offset, uint32_t slab_size);

            DataRow &insert(uint64_t key);

            DataRow &operator[](uint64_t key);

            DataRow *find(uint64_t);

            inline uint32_t size() { return map_.size(); }

            unique_ptr<Iterator<pair<uint64_t, DataRow &> &>> map_iterator();
        };

#define CMAP_SLAB_SIZE_ 65536

        struct MapAnchor {
            uint32_t index_;
            uint32_t offset_;
            MemDataRowPointer accessor_;
        };

        template<typename KEY, typename MAP>
        class CMemRowMap {
        protected:
            ThreadLocal<MapAnchor> anchor_;

            MAP map_;
            vector<shared_ptr<vector<uint64_t>>> memory_;
            uint32_t slab_size_;
            vector<uint32_t> col_offset_;
            uint32_t memory_watermark_;
            mutex memory_lock_;

            uint32_t row_size_;

            void new_slab();

            void init();

        public:
            CMemRowMap(uint32_t, const vector<uint32_t> &);

            CMemRowMap(uint32_t, const vector<uint32_t> &, uint32_t);

            DataRow &insert(KEY key);

            DataRow &operator[](KEY key);

            DataRow *find(KEY key);

            bool test(KEY key);

            DataRow *remove(KEY key);

            uint32_t size();

            unique_ptr<Iterator<pair<KEY, DataRow &> &>> map_iterator();
        };

        using namespace container;

        template
        class CMemRowMap<int32_t, PhaseConcurrentIntHashMap>;

        template
        class CMemRowMap<int64_t, PhaseConcurrentInt64HashMap>;

        using CInt32MemRowMap =
        class CMemRowMap<int32_t, PhaseConcurrentIntHashMap>;

        using CInt64MemRowMap =
        class CMemRowMap<int64_t, PhaseConcurrentInt64HashMap>;
    }

}


#endif //ARROW_DATA_CONTAINER_H
