//
// Created by Harper on 6/6/20.
//

#ifndef LQF_ROW_COPY_H
#define LQF_ROW_COPY_H

#include <memory>
#include <initializer_list>
#include <array>
#include "data_model.h"
#include "memorypool.h"

namespace lqf {
    namespace rowcopy {

        using namespace std;

        enum FIELD_TYPE {
            F_REGULAR, F_STRING, F_RAW
        };

        struct FieldInst {
            FIELD_TYPE type_;
            uint32_t from_;
            uint32_t to_;

            FieldInst(FIELD_TYPE, uint32_t, uint32_t);
        };

        class FunctorBase {
        protected:
            vector<function<void(DataRow &, DataRow &)>> contents_;

        public:
            void add(function<void(DataRow &, DataRow &)> &&f);

            void operator()(DataRow &to, DataRow &from);
        };

        class Snapshoter : public FunctorBase {
        protected:
            vector<uint32_t> col_offset_;
        public:
            Snapshoter(vector<uint32_t> &col_offset);

            unique_ptr<MemDataRow> operator()(DataRow &input);

            void operator()(DataRow &to, DataRow &from);

            inline vector<uint32_t> &colOffset() { return col_offset_; }
        };

        /**
         * Generate rowcopy objects
         */
        class RowCopyFactory {
        protected:
            TABLE_TYPE from_type_;
            TABLE_TYPE to_type_;
            vector<uint32_t> from_offset_;
            vector<uint32_t> to_offset_;
            vector<FieldInst> fields_;
            vector<function<void(DataRow &, DataRow &)>> processors_;

            void buildInternal(FunctorBase &);

        public:
            RowCopyFactory *from(TABLE_TYPE from);

            RowCopyFactory *to(TABLE_TYPE to);

            RowCopyFactory *from_layout(const vector<uint32_t> &offset);

            RowCopyFactory *to_layout(const vector<uint32_t> &offset);

            RowCopyFactory *field(FIELD_TYPE type, uint32_t from, uint32_t to);

            RowCopyFactory *process(function<void(DataRow &, DataRow &)>);

            RowCopyFactory *layout_snapshot();

            unique_ptr<function<void(DataRow &, DataRow &)>> build();

            unique_ptr<function<void(DataRow &, DataRow &)>>
            buildAssign(TABLE_TYPE from, TABLE_TYPE to, uint32_t num_fields);

            unique_ptr<function<void(DataRow &, DataRow &)>>
            buildAssign(TABLE_TYPE from, TABLE_TYPE to, vector<uint32_t> &col_size);

            unique_ptr<Snapshoter> buildSnapshot();
        };

        namespace elements {

            inline void rc_memcpy(DataRow &to, DataRow &from) {
                memcpy(to.raw(), from.raw(), to.size() * sizeof(uint64_t));
            }

            inline void
            rc_fieldmemcpy(DataRow &to, DataRow &from, uint32_t from_start, uint32_t to_start, uint32_t len) {
                memcpy((void *) (to.raw() + to_start), (void *) (from.raw() + from_start), len * sizeof(uint64_t));
            }

            inline void rc_field(DataRow &to, DataRow &from, uint32_t to_idx, uint32_t from_idx) {
                to[to_idx] = from[from_idx];
            }

            inline void rc_extfield(DataRow &to, DataRow &from, uint32_t to_idx, uint32_t from_idx) {
                DataField &tofield = to[to_idx];
                tofield = from[from_idx];
                memory::ByteArrayBuffer::instance.allocate(tofield.asByteArray());
            }

            inline void rc_rawfield(DataRow &to, DataRow &from, uint32_t to_idx, uint32_t from_idx) {
                to[to_idx] = from(from_idx);
            }
        }
    }
}


#endif //ARROW_ROW_COPY_H
