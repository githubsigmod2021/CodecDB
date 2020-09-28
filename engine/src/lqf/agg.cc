//
// Created by Harper on 6/17/20.
//

#include "agg.h"
#include <cstring>

namespace lqf {
    using namespace rowcopy;
    namespace agg {

        double AsDouble::MAX = INT32_MAX;
        double AsDouble::MIN = INT32_MIN;
        int32_t AsInt::MAX = INT32_MAX;
        int32_t AsInt::MIN = INT32_MIN;

        AggField::AggField(uint32_t size, uint32_t read_idx, bool need_dump)
                : size_(size), read_idx_(read_idx), need_dump_(need_dump) {}

        void AggField::attach(DataRow &target) {
            value_ = target.raw() + write_idx_;
        }

        void AggField::dump() { // Do nothing by default
        }

        void AggField::init() {
            value_ = 0;
        }

        Count::Count() : AggField(1, 0) {}

        void Count::reduce(DataRow &input) {
            *value_.pointer_.ival_ += 1;
        }

        void Count::merge(AggField &another) {
            *value_.pointer_.ival_ += static_cast<Count &>(another).value_.asInt();
        }

        IntDistinctCount::IntDistinctCount(uint32_t read_idx)
                : AggField(1, read_idx, true) {}

        void IntDistinctCount::attach(DataRow &target) {
            AggField::attach(target);
            distinct_ = reinterpret_cast<unordered_set<int32_t> *>(*value_.pointer_.raw_);
        }

        void IntDistinctCount::init() {
            distinct_ = new unordered_set<int32_t>();
            *value_.pointer_.raw_ = reinterpret_cast<uint64_t>(distinct_);
        }

        void IntDistinctCount::reduce(DataRow &input) {
            distinct_->insert(input[read_idx_].asInt());
        }

        void IntDistinctCount::merge(AggField &another) {
            auto &a = static_cast<IntDistinctCount &>(another);
            distinct_->insert(a.distinct_->begin(), a.distinct_->end());
            delete a.distinct_;
        }

        void IntDistinctCount::dump() {
            auto size = distinct_->size();
            delete distinct_;
            value_ = static_cast<int32_t>(size);
        }

        IntSum::IntSum(uint32_t read_idx) : AggField(1, read_idx) {}

        void IntSum::reduce(DataRow &input) {
            *value_.pointer_.ival_ += input[read_idx_].asInt();
        }

        void IntSum::merge(AggField &another) {
            *value_.pointer_.ival_ += static_cast<IntSum &>(another).value_.asInt();
        }

        DoubleSum::DoubleSum(uint32_t read_idx) : AggField(1, read_idx) {}

        void DoubleSum::reduce(DataRow &input) {
            *value_.pointer_.dval_ += input[read_idx_].asDouble();
        }

        void DoubleSum::merge(AggField &another) {
            *value_.pointer_.dval_ += static_cast<DoubleSum &>(another).value_.asDouble();
        }

        template<typename ACC>
        Avg<ACC>::Avg(uint32_t read_idx) : AggField(2, read_idx, true) {}

        template<typename ACC>
        void Avg<ACC>::attach(DataRow &target) {
            AggField::attach(target);
            count_ = target.raw() + write_idx_ + 1;
        }

        template<typename ACC>
        void Avg<ACC>::reduce(DataRow &input) {
            value_ = ACC::get(value_) + ACC::get(input[read_idx_]);
            *count_.pointer_.ival_ += 1;
        }

        template<typename ACC>
        void Avg<ACC>::merge(AggField &another) {
            Avg<ACC> &anotherIa = static_cast<Avg<ACC> &>(another);
            value_ = ACC::get(value_) + ACC::get(anotherIa.value_);
            *count_.pointer_.ival_ += anotherIa.count_.asInt();
        }

        template<typename ACC>
        void Avg<ACC>::dump() {
            auto value = ACC::get(value_);
            auto count = count_.asInt();
            value_ = static_cast<double>(value) / count;
        }

        template<typename ACC>
        void Avg<ACC>::init() {
            AggField::init();
            count_ = 0;
        }

        template<typename ACC>
        Max<ACC>::Max(uint32_t read_idx) : AggField(1, read_idx) {}

        template<typename ACC>
        void Max<ACC>::init() {
            value_ = ACC::MIN;
        }

        template<typename ACC>
        void Max<ACC>::reduce(DataRow &input) {
            value_ = std::max(ACC::get(value_), ACC::get(input[read_idx_]));
        }

        template<typename ACC>
        void Max<ACC>::merge(AggField &another) {
            value_ = std::max(ACC::get(value_), ACC::get(static_cast<Max<ACC> &>(another).value_));
        }

        template<typename ACC>
        Min<ACC>::Min(uint32_t read_idx) : AggField(1, read_idx) {}

        template<typename ACC>
        void Min<ACC>::init() {
            value_ = ACC::MAX;
        }

        template<typename ACC>
        void Min<ACC>::reduce(DataRow &input) {
            value_ = std::min(ACC::get(value_), ACC::get(input[read_idx_]));
        }

        template<typename ACC>
        void Min<ACC>::merge(AggField &another) {
            value_ = std::min(ACC::get(value_), ACC::get(static_cast<Min<ACC> &>(another).value_));
        }

        AggReducer::AggReducer(const vector<uint32_t> &offset, Snapshoter *header_copier,
                               vector<AggField *> fields,
                               const vector<uint32_t> &fields_offset)
                : storage_(offset), header_copier_(header_copier) {
            int i = 0;
            for (auto &field: fields) {
                fields_.push_back(unique_ptr<AggField>(field));
                field->write_at(fields_offset[i++]);
            }
        }

        AggReducer::AggReducer(const vector<uint32_t> &offset, Snapshoter *header_copier,
                               AggField *field, uint32_t field_offset)
                : storage_(offset), header_copier_(header_copier) {
            fields_.push_back(unique_ptr<AggField>(field));
            field->write_at(field_offset);
        }

        void AggReducer::attach(uint64_t *pointer) {
            storage_.raw(pointer);
            // Attach each field
            for (auto &field: fields_) {
                field->attach(storage_);
            }
        }

        void AggReducer::init(DataRow &input) {
            (*header_copier_)(storage_, input);
            // Init the storage content as 0
            for (auto &field:fields_) {
                field->init();
            }
            reduce(input);
        }


        void AggReducer::reduce(DataRow &input) {
            for (auto &field: fields_) {
                field->reduce(input);
            }
        }

        void AggReducer::dump() {
            for (auto &field: fields_) {
                field->dump();
            }
        }

        void AggReducer::merge(AggReducer &another) {
            for (auto i = 0u; i < fields_.size(); ++i) {
                fields_[i]->merge(*another.fields_[i]);
            }
        }

        CoreBase::CoreBase(function<void(DataRow &, DataRow &)> *row_copier, bool need_dump)
                : row_copier_(move(row_copier)), need_dump_(need_dump) {}

        HashLargeCore::HashLargeCore(const vector<uint32_t> &col_offset, unique_ptr<AggReducer> reducer,
                                     function<uint64_t(DataRow &)> &hasher,
                                     function<void(DataRow &, DataRow &)> *row_copier, bool need_dump)
                : CoreBase(move(row_copier), need_dump), reducer_(move(reducer)), hasher_(hasher), map_(col_offset) {}

        void HashLargeCore::reduce(DataRow &row) {
            auto key = hasher_(row);
            DataRow *exist = map_.find(key);
            if (exist) {
                reducer_->attach(exist->raw());
                reducer_->reduce(row);
            } else {
                reducer_->attach(map_.insert(key).raw());
                reducer_->init(row);
            }
        }

        void HashLargeCore::merge(HashLargeCore &another) {
            auto ite = another.map_.map_iterator();
            while (ite->hasNext()) {
                auto &next = ite->next();
                another.reducer_->attach(next.second.raw());
                auto exist = map_.find(next.first);
                if (exist) {
                    reducer_->attach(exist->raw());
                    reducer_->merge(*another.reducer_);
                } else {
                    DataRow &inserted = map_.insert(next.first);
                    reducer_->attach(inserted.raw());
                    (*row_copier_)(*reducer_->storage(), *another.reducer_->storage());
                }
            }
        }

        void HashLargeCore::dump(MemTable &table, function<bool(DataRow &)> pred) {
            if (pred == nullptr) {
                if (!table.isVertical()) {
                    auto flexblock = table.allocateFlex();

                    if (need_dump_) {
                        auto iterator = map_.iterator();
                        while (iterator->hasNext()) {
                            DataRow &next = iterator->next();
                            reducer_->attach(next.raw());
                            reducer_->dump();
                        }
                    }
                    flexblock->assign(map_.memory(), map_.slab_size(), map_.size());
                } else {
                    auto block = table.allocate(map_.size());
                    auto writer = block->rows();
                    if (need_dump_) {
                        auto iterator = map_.iterator();
                        while (iterator->hasNext()) {
                            DataRow &next = iterator->next();
                            reducer_->attach(next.raw());
                            reducer_->dump();
                            (*row_copier_)(writer->next(), *reducer_->storage());
                        }
                    } else {
                        auto iterator = map_.iterator();
                        while (iterator->hasNext()) {
                            DataRow &next = iterator->next();
                            reducer_->attach(next.raw());
                            (*row_copier_)(writer->next(), *reducer_->storage());
                        }
                    }
                }
            } else {
                auto block = table.allocate(map_.size());
                auto writer = block->rows();
                int counter = 0;
                if (need_dump_) {
                    auto iterator = map_.iterator();
                    while (iterator->hasNext()) {
                        DataRow &next = iterator->next();
                        reducer_->attach(next.raw());
                        reducer_->dump();
                        if (pred(*reducer_->storage())) {
                            (*row_copier_)(writer->next(), *reducer_->storage());
                            ++counter;
                        }
                    }
                } else {
                    auto iterator = map_.iterator();
                    while (iterator->hasNext()) {
                        DataRow &next = iterator->next();
                        reducer_->attach(next.raw());
                        if (pred(*reducer_->storage())) {
                            (*row_copier_)(writer->next(), *reducer_->storage());
                            ++counter;
                        }
                    }
                }
                block->resize(counter);
            }
        }

        HashCore::HashCore(const vector<uint32_t> &col_offset, function<unique_ptr<AggReducer>()> reducer_gen,
                           function<uint64_t(DataRow &)> &hasher,
                           function<void(DataRow &, DataRow &)> *row_copier, bool need_dump)
                : CoreBase(move(row_copier), need_dump), reducer_gen_(reducer_gen), hasher_(hasher),
                  col_offset_(col_offset), rows_(col_offset, 16384) {}

        void HashCore::reduce(DataRow &row) {
            auto key = hasher_(row);
            auto found = map_.find(key);
            if (__builtin_expect(found != map_.cend(), 1)) {
                found->second->reduce(row);
            } else {
                DataRow &newstorage = rows_.push_back();
                auto reducer = reducer_gen_();
                reducer->attach(newstorage.raw());
                reducer->init(row);
                map_[key] = move(reducer);
            }
        }

        void HashCore::merge(HashCore &another) {
            for (auto &ite: another.map_) {
                auto exist = map_.find(ite.first);
                if (exist != map_.cend()) {
                    exist->second->merge(*ite.second);
                } else {
                    DataRow &storage = rows_.push_back();
                    memcpy((void *) storage.raw(), (void *) ite.second->storage()->raw(),
                           sizeof(uint64_t) * storage.size());
                    auto &reducer = ite.second;
                    reducer->attach(storage.raw());
                    map_[ite.first] = move(reducer);
                }
            }
        }

        void HashCore::dump(MemTable &table, function<bool(DataRow &)> pred) {
            auto block = table.allocate(map_.size());
            auto writerows = block->rows();
            if (!pred) {
                if (need_dump_) {
                    for (auto &iterator:map_) {
                        iterator.second->dump();
                        DataRow &next = *(iterator.second->storage());
                        DataRow &to = writerows->next();
                        (*row_copier_)(to, next);
                    }
                } else {
                    for (auto &iterator:map_) {
                        DataRow &next = *(iterator.second->storage());
                        DataRow &to = writerows->next();
                        (*row_copier_)(to, next);
                    }
                }
            } else {
                int counter = 0;
                if (need_dump_) {
                    for (auto &iterator:map_) {
                        iterator.second->dump();
                        DataRow &next = *(iterator.second->storage());
                        if (pred(next)) {
                            DataRow &to = writerows->next();
                            (*row_copier_)(to, next);
                            ++counter;
                        }
                    }
                } else {
                    for (auto &iterator:map_) {
                        DataRow &next = *(iterator.second->storage());
                        if (pred(next)) {
                            DataRow &to = writerows->next();
                            (*row_copier_)(to, next);
                            ++counter;
                        }
                    }
                }
                block->resize(counter);
            }
        }

        TableCore::TableCore(uint32_t table_size, const vector<uint32_t> &col_offset,
                             function<unique_ptr<AggReducer>()> reducer_gen, function<uint32_t(DataRow &)> &indexer,
                             function<void(DataRow &, DataRow &)> *row_copier, bool need_dump)
                : CoreBase(move(row_copier), need_dump), reducer_gen_(reducer_gen), indexer_(indexer),
                  col_offset_(col_offset), table_(table_size),
                  rows_(col_offset, 16384) {}

        void TableCore::reduce(DataRow &row) {
            auto key = indexer_(row);
            auto &found = table_[key];
            if (__builtin_expect(found != NULL, 1)) {
                found->reduce(row);
            } else {
                DataRow &newstorage = rows_.push_back();
                auto reducer = reducer_gen_();
                reducer->attach(newstorage.raw());
                reducer->init(row);
                table_[key] = move(reducer);
            }
        }

        void TableCore::merge(TableCore &another) {
            auto table_size = table_.size();

            for (uint32_t i = 0; i < table_size; ++i) {
                auto &target = another.table_[i];
                auto &exist = table_[i];
                if (target != NULL) {
                    if (exist != NULL) {
                        exist->merge(*target);
                    } else {
                        DataRow &storage = rows_.push_back();
                        memcpy((void *) storage.raw(), (void *) target->storage()->raw(),
                               sizeof(uint64_t) * storage.size());
                        target->attach(storage.raw());
                        table_[i] = move(target);
                    }
                }
            }
        }

        void TableCore::dump(MemTable &table, function<bool(DataRow &)> pred) {
            auto block = table.allocateFlex();
            for (auto &iterator:table_) {
                if (iterator != NULL) {
                    if (need_dump_) {
                        iterator->dump();
                    }
                    DataRow &next = *(iterator->storage());
                    if (!pred || pred(next)) {
                        DataRow &to = block->push_back();
                        (*row_copier_)(to, next);
                    }
                }
            }
        }

        SimpleCore::SimpleCore(const vector<uint32_t> &col_offset, unique_ptr<AggReducer> reducer,
                               function<void(DataRow &, DataRow &)> *row_copier, bool need_dump)
                : CoreBase(move(row_copier), need_dump), reducer_(move(reducer)), storage_(col_offset) {
            reducer_->attach(storage_.raw());
            for (auto &field:reducer_->fields()) {
                field->init();
            }
        }

        void SimpleCore::reduce(DataRow &row) {
            reducer_->reduce(row);
        }

        void SimpleCore::merge(SimpleCore &another) {
            reducer_->merge(*another.reducer_);
        }

        void SimpleCore::dump(MemTable &table, function<bool(DataRow &)> pred) {
            // Ignore predicate
            if (need_dump_) {
                reducer_->dump();
            }
            auto block = table.allocate(1);
            auto row = block->rows();
            (*row_copier_)(row->next(), storage_);
        }

        HashStrCore::HashStrCore(const vector<uint32_t> &col_offset, function<unique_ptr<AggReducer>()> reducer_gen,
                           function<string(DataRow &)> &hasher,
                           function<void(DataRow &, DataRow &)> *row_copier, bool need_dump)
                : CoreBase(move(row_copier), need_dump), reducer_gen_(reducer_gen), hasher_(hasher),
                  col_offset_(col_offset), rows_(col_offset, 16384) {}

        void HashStrCore::reduce(DataRow &row) {
            auto key = hasher_(row);
            auto found = map_.find(key);
            if (__builtin_expect(found != map_.cend(), 1)) {
                found->second->reduce(row);
            } else {
                DataRow &newstorage = rows_.push_back();
                auto reducer = reducer_gen_();
                reducer->attach(newstorage.raw());
                reducer->init(row);
                map_[key] = move(reducer);
            }
        }

        void HashStrCore::merge(HashStrCore &another) {
            for (auto &ite: another.map_) {
                auto exist = map_.find(ite.first);
                if (exist != map_.cend()) {
                    exist->second->merge(*ite.second);
                } else {
                    DataRow &storage = rows_.push_back();
                    memcpy((void *) storage.raw(), (void *) ite.second->storage()->raw(),
                           sizeof(uint64_t) * storage.size());
                    auto &reducer = ite.second;
                    reducer->attach(storage.raw());
                    map_[ite.first] = move(reducer);
                }
            }
        }

        void HashStrCore::dump(MemTable &table, function<bool(DataRow &)> pred) {
            auto block = table.allocate(map_.size());
            auto writerows = block->rows();
            if (!pred) {
                if (need_dump_) {
                    for (auto &iterator:map_) {
                        iterator.second->dump();
                        DataRow &next = *(iterator.second->storage());
                        DataRow &to = writerows->next();
                        (*row_copier_)(to, next);
                    }
                } else {
                    for (auto &iterator:map_) {
                        DataRow &next = *(iterator.second->storage());
                        DataRow &to = writerows->next();
                        (*row_copier_)(to, next);
                    }
                }
            } else {
                int counter = 0;
                if (need_dump_) {
                    for (auto &iterator:map_) {
                        iterator.second->dump();
                        DataRow &next = *(iterator.second->storage());
                        if (pred(next)) {
                            DataRow &to = writerows->next();
                            (*row_copier_)(to, next);
                            ++counter;
                        }
                    }
                } else {
                    for (auto &iterator:map_) {
                        DataRow &next = *(iterator.second->storage());
                        if (pred(next)) {
                            DataRow &to = writerows->next();
                            (*row_copier_)(to, next);
                            ++counter;
                        }
                    }
                }
                block->resize(counter);
            }
        }

        namespace recording {

            RecordingAggField::RecordingAggField(uint32_t
                                                 read_idx, uint32_t
                                                 key_idx)
                    : AggField(2, read_idx), key_idx_(key_idx) {}

            void RecordingAggField::attach(DataRow &input) {
                AggField::attach(input);
                keys_ = reinterpret_cast<unordered_set<int32_t> *>(*(this->value_.data() + 1));
            }

            void RecordingAggField::init() {
                keys_ = new unordered_set<int32_t>();
                *(this->value_.data() + 1) = reinterpret_cast<uint64_t>(keys_);
            }

            void RecordingAggField::merge(AggField &another) {
                auto &arf = static_cast<RecordingAggField &>(another);
                delete arf.keys_;
            }

            template<typename ACC>
            RecordingMin<ACC>::RecordingMin(uint32_t
                                            read_idx, uint32_t
                                            key_idx)
                    : RecordingAggField(read_idx, key_idx) {}

            template<typename ACC>
            void RecordingMin<ACC>::init() {
                RecordingAggField::init();
                value_ = ACC::MAX;
            }

            template<typename ACC>
            void RecordingMin<ACC>::reduce(DataRow &input) {
                auto newval = ACC::get(input[read_idx_]);
                auto current = ACC::get(value_);
                if (newval < current) {
                    keys_->clear();
                    keys_->insert(input[key_idx_].asInt());
                    value_ = newval;
                } else if (newval == current) {
                    keys_->insert(input[key_idx_].asInt());
                }
            }

            template<typename ACC>
            void RecordingMin<ACC>::merge(AggField &a) {
                RecordingMin<ACC> &another = static_cast<RecordingMin<ACC> &>(a);
                auto myvalue = ACC::get(value_);
                auto othervalue = ACC::get(another.value_);

                if (myvalue > othervalue) {
                    keys_->clear();
                }
                if (myvalue >= othervalue) {
                    value_ = othervalue;
                    keys_->insert(another.keys_->begin(), another.keys_->end());
                }
                RecordingAggField::merge(a);
            }

            template<typename ACC>
            RecordingMax<ACC>::RecordingMax(uint32_t read_idx, uint32_t key_idx)
                    : RecordingAggField(read_idx, key_idx) {}

            template<typename ACC>
            void RecordingMax<ACC>::init() {
                RecordingAggField::init();
                value_ = ACC::MIN;
            }

            template<typename ACC>
            void RecordingMax<ACC>::reduce(DataRow &input) {
                auto newval = ACC::get(input[read_idx_]);
                auto current = ACC::get(value_);
                if (newval > current) {
                    keys_->clear();
                    keys_->insert(input[key_idx_].asInt());
                    value_ = newval;
                } else if (newval == current) {
                    keys_->insert(input[key_idx_].asInt());
                }
            }

            template<typename ACC>
            void RecordingMax<ACC>::merge(AggField &a) {
                RecordingMax<ACC> &another = static_cast<RecordingMax<ACC> &>(a);
                auto myvalue = ACC::get(value_);
                auto othervalue = ACC::get(another.value_);

                if (myvalue < othervalue) {
                    keys_->clear();
                }
                if (myvalue <= othervalue) {
                    value_ = othervalue;
                    keys_->insert(another.keys_->begin(), another.keys_->end());
                }
                RecordingAggField::merge(a);
            }

            RecordingHashCore::RecordingHashCore(const vector<uint32_t> &col_offset,
                                                 function<unique_ptr<AggReducer>()> reducer_gen,
                                                 function<uint64_t(DataRow &)> &hasher,
                                                 function<void(DataRow &, DataRow &)> *row_copier)
                    : HashCore(col_offset, reducer_gen, hasher, move(row_copier), false),
                      write_key_index_(col_offset.back() - 1) {}

            void RecordingHashCore::dump(MemTable &table, function<bool(DataRow &)> pred) {
                assert(pred == nullptr);
                auto block = table.allocate(map_.size());
                auto writerows = block->rows();
                auto block_size = block->size();
                auto count = 0u;

                for (auto &entry: map_) {
                    auto &reducer = entry.second;
                    auto next = reducer->storage();
                    auto field = static_cast<RecordingAggField *>(reducer->fields()[0].get());
                    auto keys = field->keys();
                    count += keys->size();
                    if (count > block_size) {
                        while (count > block_size) {
                            block_size = block_size * 1.5;
                        }
                        block->resize(block_size);
                    }

                    for (auto &key: *keys) {
                        DataRow &writeto = writerows->next();
                        (*row_copier_)(writeto, *next);
                        writeto[write_key_index_] = key;
                    }
                    delete keys;
                }
                block->resize(count);
            }

            RecordingSimpleCore::RecordingSimpleCore(const vector<uint32_t> &col_offset, unique_ptr<AggReducer> reducer,
                                                     function<void(DataRow &, DataRow &)> *row_copier)
                    : SimpleCore(col_offset, move(reducer), move(row_copier), false),
                      write_key_index_(col_offset.back() - 1) {}

            void RecordingSimpleCore::dump(MemTable &table, function<bool(DataRow &)> pred) {
                auto flex = table.allocateFlex();
                auto field = static_cast<RecordingAggField *>(reducer_->fields()[0].get());
                if (!pred) {
                    auto keys = field->keys();
                    for (auto &key:*keys) {
                        DataRow &writeto = flex->push_back();
                        (*row_copier_)(writeto, storage_);
                        writeto[write_key_index_] = key;
                    }
                    delete keys;
                } else {
                    MemDataRow buffer(table.colOffset());
                    auto keys = field->keys();
                    for (auto &key:*keys) {
                        (*row_copier_)(buffer, storage_);
                        buffer[write_key_index_] = key;
                        if (pred(buffer)) {
                            DataRow &writeto = flex->push_back();
                            (*row_copier_)(writeto, buffer);
                        }
                    }
                    delete keys;
                }
            }
        }
    }

    template<typename CORE>
    Agg<CORE>::Agg(unique_ptr<Snapshoter> header_copier, function<vector<AggField *>()> fields_gen,
                   function<bool(DataRow &)> pred, bool vertical)
            : Node(1), header_copier_(move(header_copier)), fields_gen_(fields_gen), predicate_(pred),
              vertical_(vertical) {
        auto &header_offset = header_copier_->colOffset();
        col_offset_.insert(col_offset_.end(), header_offset.begin(), header_offset.end());

        auto fields = fields_gen_();
        need_field_dump_ = false;
        for (auto field: fields) {
            fields_start_.push_back(col_offset_.back());
            col_offset_.push_back(col_offset_.back() + field->size());
            need_field_dump_ |= field->need_dump();
            delete field;
        }
        col_size_ = offset2size(col_offset_);
        row_copier_ = RowCopyFactory().buildAssign(RAW, vertical_ ? OTHER : RAW, col_offset_);
    }

    template<typename CORE>
    Agg<CORE>::Agg(unique_ptr<Snapshoter> header_copier, function<RecordingAggField *()> field_gen,
                   function<bool(DataRow &)> pred, bool vertical)
            : Node(1), header_copier_(move(header_copier)), field_gen_(field_gen), predicate_(pred),
              vertical_(vertical) {
        auto &header_offset = header_copier_->colOffset();
        col_offset_.insert(col_offset_.end(), header_offset.begin(), header_offset.end());

        need_field_dump_ = false;

        fields_start_.push_back(col_offset_.back());
        col_offset_.push_back(col_offset_.back() + 1);
        col_offset_.push_back(col_offset_.back() + 1);

        col_size_ = offset2size(col_offset_);
        row_copier_ = RowCopyFactory().buildAssign(RAW, vertical_ ? OTHER : RAW, col_offset_);
    }

    template<class CORE>
    unique_ptr<AggReducer> Agg<CORE>::createReducer() {
        return unique_ptr<AggReducer>(
                new AggReducer(col_offset_, header_copier_.get(), fields_gen_(), fields_start_));
    }

    template<class CORE>
    unique_ptr<AggReducer> Agg<CORE>::createRecordingReducer() {
        return unique_ptr<AggReducer>(
                new AggReducer(col_offset_, header_copier_.get(), field_gen_(), fields_start_.front()));
    }

    template<typename CORE>
    unique_ptr<NodeOutput> Agg<CORE>::execute(const vector<NodeOutput *> &input) {
        auto input0 = static_cast<TableOutput *>(input[0]);
        auto result = agg(*(input0->get()));
        return unique_ptr<TableOutput>(new TableOutput(result));
    }

    using namespace std::placeholders;

    template<typename CORE>
    shared_ptr<Table> Agg<CORE>::agg(Table &input) {
        function<shared_ptr<CORE>(
                const shared_ptr<Block> &)> mapper = bind(&Agg::processBlock, this, _1);

        auto reducer = [](const shared_ptr<CORE> &a, const shared_ptr<CORE> &b) {
            a->merge(*b);
            return move(a);
        };
        auto merged = input.blocks()->map(mapper)->reduce(reducer);

        auto result = MemTable::Make(col_size_, vertical_);
        merged->dump(*result, predicate_);

        return result;
    }

    template<typename CORE>
    shared_ptr<CORE> Agg<CORE>::processBlock(const shared_ptr<Block> &block) {
        auto rows = block->rows();
        auto core = makeCore();
        uint64_t blockSize = block->size();

        for (uint32_t i = 0; i < blockSize; ++i) {
            core->reduce(rows->next());
        }
        return move(core);
    }

// Subclass should override this
    template<typename CORE>
    shared_ptr<CORE> Agg<CORE>::makeCore() { return nullptr; }


    HashLargeAgg::HashLargeAgg(function<uint64_t(DataRow &)> hasher, unique_ptr<Snapshoter> header_copier,
                               function<vector<agg::AggField *>()> fields_gen,
                               function<bool(DataRow &)> pred, bool vertical)
            : Agg(move(header_copier), fields_gen, pred, vertical), hasher_(hasher) {}

    shared_ptr<HashLargeCore> HashLargeAgg::makeCore() {
        return make_shared<HashLargeCore>(col_offset_, createReducer(), hasher_, row_copier_.get(), need_field_dump_);
    }

    HashAgg::HashAgg(function<uint64_t(DataRow &)> hasher, unique_ptr<Snapshoter> header_copier,
                     function<vector<agg::AggField *>()> fields_gen,
                     function<bool(DataRow &)> pred, bool vertical)
            : Agg(move(header_copier), fields_gen, pred, vertical), hasher_(hasher) {}

    shared_ptr<HashCore> HashAgg::makeCore() {
        function<unique_ptr<AggReducer>()> rc = bind(&HashAgg::createReducer, this);
        return make_shared<HashCore>(col_offset_, rc, hasher_, row_copier_.get(), need_field_dump_);
    }

    TableAgg::TableAgg(uint32_t table_size, function<uint32_t(DataRow &)> indexer, unique_ptr<Snapshoter> header_copier,
                       function<vector<agg::AggField *>()> fields_gen, function<bool(DataRow &)> pred, bool vertical)
            : Agg(move(header_copier), fields_gen, pred, vertical), table_size_(table_size), indexer_(indexer) {}

    shared_ptr<TableCore> TableAgg::makeCore() {
        function<unique_ptr<AggReducer>()> rc = bind(&TableAgg::createReducer, this);
        return make_shared<TableCore>(table_size_, col_offset_, rc, indexer_, row_copier_.get(), need_field_dump_);
    }

    SimpleAgg::SimpleAgg(function<vector<agg::AggField *>()> fields_gen, function<bool(DataRow &)> pred,
                         bool vertical)
            : Agg(RowCopyFactory().buildSnapshot(), fields_gen, pred, vertical) {}

    shared_ptr<SimpleCore> SimpleAgg::makeCore() {
        return make_shared<SimpleCore>(col_offset_, createReducer(), row_copier_.get(), need_field_dump_);
    }

    HashStrAgg::HashStrAgg(function<string(DataRow &)> hasher, unique_ptr<Snapshoter> header_copier,
                     function<vector<agg::AggField *>()> fields_gen,
                     function<bool(DataRow &)> pred, bool vertical)
            : Agg(move(header_copier), fields_gen, pred, vertical), hasher_(hasher) {}

    shared_ptr<HashStrCore> HashStrAgg::makeCore() {
        function<unique_ptr<AggReducer>()> rc = bind(&HashStrAgg::createReducer, this);
        return make_shared<HashStrCore>(col_offset_, rc, hasher_, row_copier_.get(), need_field_dump_);
    }

    RecordingHashAgg::RecordingHashAgg(function<uint64_t(DataRow &)> hasher, unique_ptr<Snapshoter> header_copier,
                                       function<RecordingAggField *()> field_gen,
                                       function<bool(DataRow &)> pred, bool vertical)
            : Agg(move(header_copier), field_gen, pred, vertical), hasher_(hasher) {}

    shared_ptr<RecordingHashCore> RecordingHashAgg::makeCore() {
        function<unique_ptr<AggReducer>()> rc = bind(&RecordingHashAgg::createRecordingReducer, this);
        return make_shared<RecordingHashCore>(col_offset_, rc, hasher_, row_copier_.get());
    }

    RecordingSimpleAgg::RecordingSimpleAgg(function<RecordingAggField *()> field_gen,
                                           function<bool(DataRow &)> pred,
                                           bool vertical)
            : Agg(RowCopyFactory().buildSnapshot(), field_gen, pred, vertical) {}

    shared_ptr<RecordingSimpleCore> RecordingSimpleAgg::makeCore() {
        return make_shared<RecordingSimpleCore>(col_offset_, createRecordingReducer(), row_copier_.get());
    }

    StripeHashAgg::StripeHashAgg(uint32_t num_stripe, function<uint64_t(DataRow &)> hasher,
                                 function<uint64_t(DataRow &)> stripe_hasher, unique_ptr<Snapshoter> stripe_copier,
                                 unique_ptr<Snapshoter> header_copier, function<vector<agg::AggField *>()> fields_gen,
                                 function<bool(DataRow &)> pred)
            : Node(1), num_stripe_(num_stripe), hasher_(hasher), stripe_hasher_(stripe_hasher),
              stripe_copier_(move(stripe_copier)), header_copier_(move(header_copier)),
              fields_gen_(fields_gen), predicate_(pred) {
        auto &header_offset = header_copier_->colOffset();
        col_offset_.insert(col_offset_.end(), header_offset.begin(), header_offset.end());
        auto fields = fields_gen_();
        need_field_dump_ = false;
        for (auto field: fields) {
            fields_start_.push_back(col_offset_.back());
            col_offset_.push_back(col_offset_.back() + field->size());
            need_field_dump_ |= field->need_dump();
            delete field;
        }
        col_size_ = offset2size(col_offset_);
        data_copier_ = RowCopyFactory().buildAssign(RAW, RAW, col_offset_);
    }

    unique_ptr<NodeOutput> StripeHashAgg::execute(const vector<NodeOutput *> &input) {
        auto input0 = static_cast<TableOutput *>(input[0]);
        auto result = agg(*(input0->get()));
        return unique_ptr<TableOutput>(new TableOutput(result));
    }

    shared_ptr<Table> StripeHashAgg::agg(Table &input) {
        function<shared_ptr<vector<shared_ptr<MemRowVector>>>(const shared_ptr<Block> &)> stripeMaker =
                bind(&StripeHashAgg::makeStripes, this, _1);
        function<shared_ptr<vector<shared_ptr<HashLargeCore>>>(
                const shared_ptr<vector<shared_ptr<MemRowVector>>> &)> stripeProcessor = bind(
                &StripeHashAgg::aggStripes, this, _1);

        auto cores = input.blocks()->map(stripeMaker)->map(stripeProcessor)->reduce(
                bind(&StripeHashAgg::mergeCore, this, _1, _2));

        auto outputTable = MemTable::Make(col_size_);
        for (auto &core: *cores) {
            core->dump(*outputTable, predicate_);
        }
        return outputTable;
    }

    shared_ptr<vector<shared_ptr<MemRowVector>>> StripeHashAgg::makeStripes(const shared_ptr<Block> &block) {
        auto block_size = block->size();
        auto rows = block->rows();
        auto stripes = make_shared<vector<shared_ptr<MemRowVector>>>();
        auto &stripe_offsets = stripe_copier_->colOffset();
        for (auto i = 0u; i < num_stripe_; ++i) {
            stripes->push_back(make_shared<MemRowVector>(stripe_offsets));
        }
        auto mask = num_stripe_ - 1;
        for (auto i = 0u; i < block_size; ++i) {
            DataRow &row = rows->next();
            auto index = hasher_(row) & mask;
            auto stripe = (*stripes)[index];
            DataRow &writeto = stripe->push_back();
            (*stripe_copier_)(writeto, row);
        }
        return stripes;
    }

    shared_ptr<vector<shared_ptr<HashLargeCore>>>
    StripeHashAgg::aggStripes(const shared_ptr<vector<shared_ptr<MemRowVector>>> &stripes) {
        auto cores = make_shared<vector<shared_ptr<HashLargeCore>>>(num_stripe_);
        for (auto i = 0u; i < num_stripe_; ++i) {
            auto stripe = (*stripes)[i];
            auto core = makeCore();
            auto ite = stripe->iterator();
            while (ite->hasNext()) {
                core->reduce(ite->next());
            }
            (*cores)[i] = core;
        }
        return cores;
    }

    shared_ptr<vector<shared_ptr<HashLargeCore>>>
    StripeHashAgg::mergeCore(shared_ptr<vector<shared_ptr<HashLargeCore>>> lefts,
                             shared_ptr<vector<shared_ptr<HashLargeCore>>> rights) {
        for (auto i = 0u; i < num_stripe_; ++i) {
            auto left = (*lefts)[i];
            auto right = (*rights)[i];
            left->merge(*right);
        }
        return lefts;
    }

    unique_ptr<AggReducer> StripeHashAgg::createReducer() {
        return unique_ptr<AggReducer>(new AggReducer(col_offset_, header_copier_.get(), fields_gen_(), fields_start_));
    }

    shared_ptr<HashLargeCore> StripeHashAgg::makeCore() {
        return make_shared<HashLargeCore>(col_offset_, createReducer(), stripe_hasher_, data_copier_.get(),
                                          need_field_dump_);
    }

}