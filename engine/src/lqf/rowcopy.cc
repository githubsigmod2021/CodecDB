//
// Created by Harper on 6/6/20.
//

#include "rowcopy.h"

namespace lqf {
    namespace rowcopy {

        class RowCopier : public FunctorBase {
        };

        void FunctorBase::add(function<void(DataRow &, DataRow &)> &&f) {
            contents_.emplace_back(f);
        }

        void FunctorBase::operator()(DataRow &to, DataRow &from) {
            for (auto &p:contents_) {
                p(to, from);
            }
        }

        Snapshoter::Snapshoter(vector<uint32_t> &col_offset)
                : col_offset_(col_offset) {}

        unique_ptr<MemDataRow> Snapshoter::operator()(DataRow &input) {
            unique_ptr<MemDataRow> result = unique_ptr<MemDataRow>(new MemDataRow(col_offset_));
            for (auto &p:contents_) {
                p(*result, input);
            }
            return result;
        }

        void Snapshoter::operator()(DataRow &to, DataRow &from) {
            FunctorBase::operator()(to, from);
        }

        RowCopyFactory *RowCopyFactory::from(TABLE_TYPE from) {
            from_type_ = from;
            return this;
        }

        RowCopyFactory *RowCopyFactory::to(TABLE_TYPE to) {
            to_type_ = to;
            return this;
        }

        RowCopyFactory *RowCopyFactory::from_layout(const vector<uint32_t> &offset) {
            from_offset_ = offset;
            return this;
        }

        RowCopyFactory *RowCopyFactory::to_layout(const vector<uint32_t> &offset) {
            to_offset_ = offset;
            return this;
        }

        FieldInst::FieldInst(FIELD_TYPE t, uint32_t f, uint32_t to)
                : type_(t), from_(f), to_(to) {}

        RowCopyFactory *RowCopyFactory::field(FIELD_TYPE type, uint32_t from, uint32_t to) {
            fields_.emplace_back(type, from, to);
            return this;
        }

        RowCopyFactory *RowCopyFactory::process(function<void(DataRow &, DataRow &)> f) {
            processors_.emplace_back(move(f));
            return this;
        }

        void RowCopyFactory::buildInternal(FunctorBase &base) {
            if (!fields_.empty()) {
                if (from_type_ == RAW && to_type_ == RAW
                    && from_offset_.size() > 1 && to_offset_.size() > 1) {
                    // Compute consecutive sections
                    std::sort(fields_.begin(), fields_.end(),
                              [](FieldInst &a, FieldInst &b) { return a.from_ < b.from_; });

                    uint32_t from_start = -1;
                    uint32_t to_start = -1;
                    uint32_t length = 0;
                    uint32_t f_prev = -1;
                    uint32_t t_prev = -1;
                    for (auto &next: fields_) {
                        if (from_start == 0xFFFFFFFF) {
                            from_start = from_offset_[next.from_];
                            to_start = to_offset_[next.to_];
                            length = from_offset_[next.from_ + 1] - from_start;
                        } else if (next.from_ == f_prev + 1 && next.to_ == t_prev + 1) {
                            length += from_offset_[next.from_ + 1] - from_offset_[next.from_];
                        } else {
                            base.add(bind(elements::rc_fieldmemcpy,
                                          placeholders::_1, placeholders::_2, from_start, to_start, length));
                            from_start = from_offset_[next.from_];
                            to_start = to_offset_[next.to_];
                            length = from_offset_[next.from_ + 1] - from_start;
                        }
                        f_prev = next.from_;
                        t_prev = next.to_;
                    }
                    base.add(bind(elements::rc_fieldmemcpy,
                                  placeholders::_1, placeholders::_2, from_start, to_start, length));
                } else {
                    for (auto &field: fields_) {
                        switch (field.type_) {
                            case F_REGULAR:
                                base.add(bind(elements::rc_field, placeholders::_1, placeholders::_2,
                                              field.to_, field.from_));
                                break;
                            case F_STRING:
                                base.add(bind(from_type_ == EXTERNAL ? elements::rc_extfield : elements::rc_field,
                                              placeholders::_1, placeholders::_2,
                                              field.to_, field.from_));
                                break;
                            case F_RAW:
                                base.add(bind(elements::rc_rawfield, placeholders::_1, placeholders::_2,
                                              field.to_, field.from_));
                                break;
                        }
                    }
                }
            }
            for (auto &p: processors_) {
                base.add(move(p));
            }
        }

        RowCopyFactory *RowCopyFactory::layout_snapshot() {
            if (to_offset_.empty()) {
                sort(fields_.begin(), fields_.end(),
                     [](FieldInst &a, FieldInst &b) { return a.to_ < b.to_; });
                // Build to_offset_
                auto last = 0u;
                to_offset_.push_back(last);
                for (auto &f: fields_) {
                    auto field_size = f.type_ == F_STRING ? 2 : 1;
                    last += field_size;
                    to_offset_.push_back(last);
                }
            }
            return this;
        }

        unique_ptr<function<void(DataRow &, DataRow &)> > RowCopyFactory::build() {
            RowCopier rc;
            buildInternal(rc);
            return unique_ptr<function<void(DataRow &, DataRow &)>>(new function<void(DataRow &, DataRow &)>(move(rc)));
        }

        unique_ptr<Snapshoter> RowCopyFactory::buildSnapshot() {
            layout_snapshot();
            auto s = unique_ptr<Snapshoter>(new Snapshoter(to_offset_));
            buildInternal(*s);
            return s;
        }

        unique_ptr<function<void(DataRow &, DataRow &)> >
        RowCopyFactory::buildAssign(TABLE_TYPE from, TABLE_TYPE to, uint32_t num_fields) {
            this->from(from);
            this->to(to);
            auto offset = colOffset(num_fields);
            this->from_layout(offset);
            this->to_layout(offset);
            for (auto i = 0u; i < num_fields; ++i) {
                this->field(F_REGULAR, i, i);
            }
            return this->build();
        }

        unique_ptr<function<void(DataRow &, DataRow &)> >
        RowCopyFactory::buildAssign(TABLE_TYPE from, TABLE_TYPE to, vector<uint32_t> &col_offset) {
            this->from(from);
            this->to(to);
            this->from_layout(col_offset);
            this->to_layout(col_offset);
            for (auto i = 0u; i < col_offset.size() - 1; ++i) {
                this->field(col_offset[i + 1] - col_offset[i] == 2 ? F_STRING : F_REGULAR, i, i);
            }
            return this->build();
        }
    }
}