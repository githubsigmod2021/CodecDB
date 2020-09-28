//
// Created by harper on 2/25/20.
//

#include "join.h"
#include "rowcopy.h"

using namespace std;
using namespace std::placeholders;
using namespace lqf::rowcopy;

namespace lqf {
    namespace join {

        JoinBuilder::JoinBuilder(initializer_list<int32_t> initList, bool needkey, bool vertical) :
                needkey_(needkey), vertical_(vertical), field_list_(initList), left_type_(OTHER),
                right_type_(OTHER) {}

        void JoinBuilder::on(Table &left, Table &right) {
            left_type_ = left.type();
            right_type_ = right.type();

            left_col_offset_ = size2offset(left.colSize());
            right_col_offset_ = size2offset(right.colSize());
        }

        void JoinBuilder::init() {
            uint32_t i = needkey_;
            uint32_t right_counter = 0;

            RowCopyFactory snapshot_factory;

            snapshot_factory.from(right_type_);
            snapshot_factory.to(RAW);

            output_col_offset_.push_back(0);
            if (needkey_) {
                output_col_offset_.push_back(1);
                output_col_size_.push_back(1);
            }
            snapshot_col_offset_.push_back(0);

            for (auto &inst: field_list_) {
                auto index = inst & 0xffff;
                bool is_string = (inst >> 17) & 1;
                bool is_raw = (inst >> 18) & 1;
                bool is_right = inst & 0x10000;

                output_col_size_.push_back(is_string ? 2 : 1);
                output_col_offset_.push_back(output_col_offset_.back() + output_col_size_.back());

                if (is_right) {
                    FIELD_TYPE ifield_type;
                    if (is_raw) {
                        ifield_type = F_RAW;
                    } else if (is_string) {
                        ifield_type = F_STRING;
                    } else {
                        ifield_type = F_REGULAR;
                    }
                    snapshot_factory.field(ifield_type, index, right_counter++);

                    snapshot_col_size_.push_back(output_col_size_.back());
                    snapshot_col_offset_.push_back(snapshot_col_offset_.back() + snapshot_col_size_.back());
                }
                ++i;
            }
            snapshot_factory.from_layout(right_col_offset_);
            snapshot_factory.to_layout(snapshot_col_offset_);
            snapshoter_ = snapshot_factory.buildSnapshot();
        }

        RowBuilder::RowBuilder(initializer_list<int32_t> fields, bool needkey, bool vertical)
                : JoinBuilder(fields, needkey, vertical) {}

        void RowBuilder::init() {
            JoinBuilder::init();
            uint32_t i = needkey_;
            uint32_t right_counter = 0;

            RowCopyFactory left_factory;
            RowCopyFactory right_factory;

            auto dest_type = vertical_ ? OTHER : RAW;
            left_factory.from(left_type_);
            left_factory.to(dest_type);
            right_factory.from(RAW);
            right_factory.to(dest_type);

            for (auto &inst: field_list_) {
                auto index = inst & 0xffff;
                bool is_string = inst >> 17;
                bool is_raw = inst >> 18;
                bool is_right = inst & 0x10000;

                if (is_right) {
                    FIELD_TYPE ofield_type;
                    if (is_raw) {
                        ofield_type = F_REGULAR;
                    } else if (is_string) {
                        ofield_type = F_STRING;
                    } else {
                        ofield_type = F_REGULAR;
                    }
                    right_factory.field(ofield_type, right_counter++, i);
                } else {
                    FIELD_TYPE field_type;
                    if (is_raw) {
                        field_type = F_RAW;
                    } else if (is_string) {
                        field_type = F_STRING;
                    } else {
                        field_type = F_REGULAR;
                    }
                    left_factory.field(field_type, index, i);
                }
                ++i;
            }

            left_factory.from_layout(left_col_offset_);
            left_factory.to_layout(output_col_offset_);

            right_factory.from_layout(snapshot_col_offset_);
            right_factory.to_layout(output_col_offset_);

            left_copier_ = left_factory.build();
            right_copier_ = right_factory.build();
        }

        void RowBuilder::build(DataRow &output, DataRow &left, DataRow &right, int key) {
            if (needkey_) {
                output[0] = key;
            }
            (*left_copier_)(output, left);
            (*right_copier_)(output, right);
        }

        ColumnBuilder::ColumnBuilder(initializer_list<int32_t> fields)
                : JoinBuilder(fields, false, true) {}

        void ColumnBuilder::init() {
            JoinBuilder::init();

            uint32_t i = 0;
            uint32_t right_counter = 0;

            for (auto &inst: field_list_) {
                auto index = inst & 0xffff;
                bool is_right = inst & 0x10000;

                if (is_right) {
                    right_merge_inst_.emplace_back(right_counter++, i);
                } else {
                    left_merge_inst_.emplace_back(index, i);
                }
                ++i;
            }
        }

        void ColumnBuilder::build(MemvBlock &output, MemvBlock &left, MemvBlock &right) {
            output.merge(left, left_merge_inst_);
            output.merge(right, right_merge_inst_);
        }
    }

    using namespace join;
    using namespace hashcontainer;

    Join::Join() : Node(2) {}

    unique_ptr<NodeOutput> Join::execute(const vector<NodeOutput *> &inputs) {
        auto left = static_cast<TableOutput *>(inputs[0]);
        auto right = static_cast<TableOutput *>(inputs[1]);

        auto result = join(*(left->get()), *(right->get()));
        return unique_ptr<TableOutput>(new TableOutput(result));
    }

    HashBasedJoin::HashBasedJoin(uint32_t leftKeyIndex, uint32_t rightKeyIndex, JoinBuilder *builder,
                                 uint32_t expect_size)
            : leftKeyIndex_(leftKeyIndex), rightKeyIndex_(rightKeyIndex),
              builder_(unique_ptr<JoinBuilder>(builder)), expect_size_(expect_size) {}


    shared_ptr<Table> HashBasedJoin::join(Table &left, Table &right) {
        builder_->on(left, right);
        builder_->init();

        container_ = HashBuilder::buildContainer(right, rightKeyIndex_, builder_->snapshoter(), expect_size_);

        function<shared_ptr<Block>(const shared_ptr<Block> &)> prober = bind(&HashBasedJoin::probe, this, _1);
        return makeTable(left.blocks()->map(prober));
    }

    shared_ptr<Block> HashBasedJoin::makeBlock(uint32_t size) {
        if (builder_->useVertical())
            return make_shared<MemvBlock>(size, builder_->outputColSize());
        else
            return make_shared<MemBlock>(size, builder_->outputColOffset());
    }

    shared_ptr<TableView> HashBasedJoin::makeTable(unique_ptr<Stream<shared_ptr<Block>>> stream) {
        return make_shared<TableView>(builder_->useVertical() ? OTHER : RAW, builder_->outputColSize(), move(stream));
    }

    HashJoin::HashJoin(uint32_t lk, uint32_t rk, lqf::RowBuilder *builder,
                       function<bool(DataRow &, DataRow &)> pred, uint32_t expect_size)
            : HashBasedJoin(lk, rk, builder, expect_size), rowBuilder_(builder), predicate_(pred) {}

    shared_ptr<Block> HashJoin::probe(const shared_ptr<Block> &leftBlock) {
        auto leftkeys = leftBlock->col(leftKeyIndex_);
        auto leftrows = leftBlock->rows();
        auto resultblock = makeBlock(leftBlock->size());
        uint32_t counter = 0;
        auto writer = resultblock->rows();

        auto left_block_size = leftBlock->size();
        if (predicate_) {
            if (outer_) {
                for (uint32_t i = 0; i < left_block_size; ++i) {
                    DataField &key = leftkeys->next();
                    auto leftval = key.asInt();
                    auto result = container_->get(leftval);
                    DataRow &leftrow = (*leftrows)[leftkeys->pos()];
                    DataRow &right = result ? *result : MemDataRow::EMPTY;
                    if (predicate_(leftrow, right)) {
                        rowBuilder_->build((*writer)[counter++], leftrow, right, leftval);
                    }
                }
            } else {
                for (uint32_t i = 0; i < left_block_size; ++i) {
                    DataField &key = leftkeys->next();
                    auto leftval = key.asInt();
                    auto result = container_->get(leftval);
                    if (result) {
                        DataRow &row = (*leftrows)[leftkeys->pos()];
                        if (predicate_(row, *result)) {
                            rowBuilder_->build((*writer)[counter++], row, *result, leftval);
                        }
                    }
                }
            }
        } else {
            if (outer_) {
                for (uint32_t i = 0; i < left_block_size; ++i) {
                    DataField &key = leftkeys->next();
                    auto leftval = key.asInt();
                    DataRow &leftrow = (*leftrows)[leftkeys->pos()];
                    auto result = container_->get(leftval);
                    DataRow &right = result ? *result : MemDataRow::EMPTY;
                    rowBuilder_->build((*writer)[counter++], leftrow, right, leftval);
                }
            } else {
                for (uint32_t i = 0; i < left_block_size; ++i) {
                    DataField &key = leftkeys->next();
                    auto leftval = key.asInt();
                    auto result = container_->get(leftval);
                    if (result) {
                        DataRow &row = (*leftrows)[leftkeys->pos()];
                        rowBuilder_->build((*writer)[counter++], row, *result, leftval);
                    }
                }
            }
        }
        resultblock->resize(counter);

        return resultblock;
    }

    FilterJoin::FilterJoin(uint32_t leftKeyIndex, uint32_t rightKeyIndex, uint32_t expect_size, bool useBitmap)
            : leftKeyIndex_(leftKeyIndex), rightKeyIndex_(rightKeyIndex), expect_size_(expect_size),
              useBitmap_(useBitmap) {}


    shared_ptr<Table> FilterJoin::join(Table &left, Table &right) {
#ifdef LQF_NODE_TIMING
        auto start = high_resolution_clock::now();
#endif
        if (useBitmap_) {
            predicate_ = HashBuilder::buildBitmapPredicate(right, rightKeyIndex_, expect_size_);
        } else {
            predicate_ = HashBuilder::buildHashPredicate(right, rightKeyIndex_, expect_size_);
        }

        function<shared_ptr<Block>(const shared_ptr<Block> &)> prober = bind(&FilterJoin::probe, this, _1);
#ifdef LQF_NODE_TIMING
        auto stop = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(stop - start);
        cout << "Filter Join " << name_ << " Time taken: " << duration.count() << " microseconds" << endl;
#endif
        return make_shared<TableView>(left.type(), left.colSize(), left.blocks()->map(prober));
    }


    shared_ptr<Block> FilterJoin::probe(const shared_ptr<Block> &leftBlock) {
        auto col = leftBlock->col(leftKeyIndex_);
        auto bitmap = make_shared<SimpleBitmap>(leftBlock->limit());
        uint32_t size = leftBlock->size();
        if (anti_) {
            for (uint32_t i = 0; i < size; ++i) {
                auto key = col->next().asInt();
                if (!predicate_->test(key)) {
                    bitmap->put(col->pos());
                }
            }
        } else {
            for (uint32_t i = 0; i < size; ++i) {
                auto key = col->next().asInt();
                if (predicate_->test(key)) {
                    bitmap->put(col->pos());
                }
            }
        }
        return leftBlock->mask(bitmap);
    }

    FilterTransformJoin::FilterTransformJoin(uint32_t lk_idx, uint32_t rk_idx, unique_ptr<Snapshoter> matchw,
                                             unique_ptr<Snapshoter> unmatchw,
                                             uint32_t expect_size, bool use_bitmap)
            : FilterJoin(lk_idx, rk_idx, expect_size, use_bitmap), match_writer_(move(matchw)),
              unmatch_writer_(move(unmatchw)) {}

    shared_ptr<Block> FilterTransformJoin::probe(const shared_ptr<Block> &left_block) {
        auto memblock = make_shared<MemFlexBlock>(match_writer_->colOffset());
        auto col = left_block->col(leftKeyIndex_);
        auto rows = left_block->rows();
        uint32_t size = left_block->size();
        if (unmatch_writer_) {
            for (uint32_t i = 0; i < size; ++i) {
                auto key = col->next().asInt();
                DataRow &row = (*rows)[col->pos()];
                DataRow &target = memblock->push_back();
                if (predicate_->test(key)) {
                    (*match_writer_)(target, row);
                } else {
                    (*unmatch_writer_)(target, row);
                }
            }
        } else {
            for (uint32_t i = 0; i < size; ++i) {
                auto key = col->next().asInt();
                if (predicate_->test(key)) {
                    DataRow &row = (*rows)[col->pos()];
                    (*match_writer_)(memblock->push_back(), row);
                }
            }
        }
        return memblock;
    }


    HashExistJoin::HashExistJoin(uint32_t leftKeyIndex, uint32_t rightKeyIndex,
                                 JoinBuilder *builder, function<bool(DataRow &, DataRow &)> pred)
            : HashBasedJoin(leftKeyIndex, rightKeyIndex, builder), predicate_(pred) {}

    shared_ptr<Table> HashExistJoin::join(Table &left, Table &right) {
        builder_->on(left, right);
        builder_->init();

        container_ = HashBuilder::buildContainer(right, rightKeyIndex_, builder_->snapshoter());

        if (predicate_) {
            auto memTable = MemTable::Make(builder_->outputColSize(), builder_->useVertical());

            function<shared_ptr<Bitmap>(const shared_ptr<Block> &)> prober = bind(
                    &HashExistJoin::probeWithPredicate, this, _1);
            auto reducer = [](const shared_ptr<Bitmap> &a, const shared_ptr<Bitmap> &b) {
                return (*a) | (*b);
            };
            auto exist = left.blocks()->map(prober)->reduce(reducer);
            auto memblock = memTable->allocate(exist->cardinality());

            auto writerows = memblock->rows();
            auto writecount = 0;

            auto bitmapite = exist->iterator();
            while (bitmapite->hasNext()) {
                (*writerows)[writecount++] = *container_->get(static_cast<int32_t>(bitmapite->next()));
            }
            return memTable;
        } else {
            function<shared_ptr<Block>(const shared_ptr<Block> &)> prober = bind(&HashExistJoin::probe, this, _1);
            return makeTable(left.blocks()->map(prober));
        }
    }

    shared_ptr<SimpleBitmap> HashExistJoin::probeWithPredicate(const shared_ptr<Block> &leftBlock) {
        auto leftkeys = leftBlock->col(leftKeyIndex_);
        auto leftrows = leftBlock->rows();

        auto left_block_size = leftBlock->size();

        shared_ptr<SimpleBitmap> local_mask = make_shared<SimpleBitmap>(container_->max() + 1);

        for (uint32_t i = 0; i < left_block_size; ++i) {
            DataField &keyfield = leftkeys->next();
            auto key = keyfield.asInt();
            if (!local_mask->check(key)) {
                auto result = container_->get(key);
                if (result != nullptr) {
                    DataRow &leftrow = (*leftrows)[leftkeys->pos()];
                    if (predicate_(leftrow, *result)) {
                        local_mask->put(key);
                    }
                }
            }
        }
        return local_mask;
    }

    shared_ptr<Block> HashExistJoin::probe(const shared_ptr<Block> &leftBlock) {
        auto resultblock = makeBlock(container_->size());
        auto leftkeys = leftBlock->col(leftKeyIndex_);
        uint32_t counter = 0;
        auto writer = resultblock->rows();
        auto left_block_size = leftBlock->size();

        for (uint32_t i = 0; i < left_block_size; ++i) {
            DataField &keyfield = leftkeys->next();
            auto result = container_->remove(keyfield.asInt());
            if (result) {
                (*writer)[counter++] = (*result);
            }
        }
        resultblock->resize(counter);
        return resultblock;
    }

    HashNotExistJoin::HashNotExistJoin(uint32_t leftKeyIndex, uint32_t rightKeyIndex, lqf::JoinBuilder *rowBuilder,
                                       function<bool(DataRow &, DataRow &)> pred) :
            HashExistJoin(leftKeyIndex, rightKeyIndex, rowBuilder, pred) {}

    shared_ptr<Table> HashNotExistJoin::join(Table &left, Table &right) {
        builder_->on(left, right);
        builder_->init();

        container_ = HashBuilder::buildContainer(right, rightKeyIndex_, builder_->snapshoter());

        auto memTable = MemTable::Make(builder_->outputColSize(), builder_->useVertical());

        if (predicate_) {
            function<shared_ptr<Bitmap>(const shared_ptr<Block> &)> prober = bind(
                    &HashNotExistJoin::probeWithPredicate, this, _1);
            auto reducer = [](const shared_ptr<Bitmap> &a, const shared_ptr<Bitmap> &b) {
                return (*a) | (*b);
            };
            auto exist = left.blocks()->map(prober)->reduce(reducer);

            auto memblock = memTable->allocate(container_->size() - exist->cardinality());

            auto writerows = memblock->rows();

            auto ite = container_->iterator();
            while (ite->hasNext()) {
                auto entry = ite->next();
                if (!exist->check(entry.first)) {
                    DataRow &writenext = writerows->next();
                    writenext = entry.second;
                }
            }
        } else {
            function<void(const shared_ptr<Block> &)> scanner =
                    bind(&HashNotExistJoin::scan, this, _1);
            left.blocks()->foreach(scanner);

            auto resultBlock = memTable->allocate(container_->size());
            auto writeRows = resultBlock->rows();
            auto iterator = container_->iterator();
            uint32_t counter = 0;
            while (iterator->hasNext()) {
                ++counter;
                if (counter > container_->size()) {
                    break;
                }
                auto entry = iterator->next();
                writeRows->next() = entry.second;
            }
        }
        return memTable;
    }

    void HashNotExistJoin::scan(const shared_ptr<Block> &leftBlock) {
        auto leftkeys = leftBlock->col(leftKeyIndex_);
        auto leftrows = leftBlock->rows();

        auto left_block_size = leftBlock->size();
        for (uint32_t i = 0; i < left_block_size; ++i) {
            DataField &keyfield = leftkeys->next();
            container_->remove(keyfield.asInt());
        }
    }

    HashColumnJoin::HashColumnJoin(uint32_t leftKeyIndex, uint32_t rightKeyIndex, lqf::ColumnBuilder *builder,
                                   uint32_t expect_size) : HashBasedJoin(leftKeyIndex, rightKeyIndex, builder,
                                                                         expect_size), columnBuilder_(builder) {}

    shared_ptr<Block> HashColumnJoin::probe(const shared_ptr<Block> &leftBlock) {
        shared_ptr<MemvBlock> leftvBlock = dynamic_pointer_cast<MemvBlock>(leftBlock);
        /// Make sure the cast is valid
        assert(leftvBlock.get() != nullptr);

        auto leftkeys = leftBlock->col(leftKeyIndex_);
        auto leftrows = leftBlock->rows();

        MemvBlock vblock(leftBlock->size(), columnBuilder_->rightColSize());
        auto writer = vblock.rows();

        auto left_block_size = leftBlock->size();
        if (outer_) {
            for (uint32_t i = 0; i < left_block_size; ++i) {
                DataField &key = leftkeys->next();
                auto leftval = key.asInt();

                auto result = container_->get(leftval);
                if (result)
                    (*writer)[i] = *result;
            }
        } else {
            for (uint32_t i = 0; i < left_block_size; ++i) {
                DataField &key = leftkeys->next();
                auto leftval = key.asInt();

                auto result = move(container_->get(leftval));
                (*writer)[i] = *result;
            }
        }

        auto newblock = makeBlock(0);
        // Merge result block with original block
        auto newvblock = static_pointer_cast<MemvBlock>(newblock);

        columnBuilder_->build(*newvblock, *leftvBlock, vblock);
        return newvblock;
    }

    HashMultiJoin::HashMultiJoin(uint32_t lk, uint32_t rk, RowBuilder *rbuilder)
            : left_key_index_(lk), right_key_index_(rk), builder_(unique_ptr<RowBuilder>(rbuilder)) {}

    void HashMultiJoin::buildmap(const shared_ptr<Block> &block) {
        auto block_size = block->size();
        auto rows = block->rows();
        for (uint32_t i = 0; i < block_size; ++i) {
            DataRow &row = rows->next();
            auto key = row[right_key_index_].asInt();
            auto snapshot = (*builder_->snapshoter())(row);
            auto found = container_.find(key);
            if (found != container_.cend()) {
                found->second->emplace_back(move(snapshot));
            } else {
                auto newentry = unique_ptr<vector<unique_ptr<MemDataRow>>>(new vector<unique_ptr<MemDataRow>>());
                newentry->emplace_back(move(snapshot));
                container_[key] = move(newentry);
            }
        }
    }

    shared_ptr<Table> HashMultiJoin::join(Table &left, Table &right) {
        builder_->on(left, right);
        builder_->init();

        function<void(const shared_ptr<Block> &)> build_map = bind(&HashMultiJoin::buildmap, this, _1);
        right.blocks()->sequential()->foreach(build_map);

        function<shared_ptr<Block>(const shared_ptr<Block> &)> prober = bind(&HashMultiJoin::probe, this, _1);

        return make_shared<TableView>(RAW, builder_->outputColSize(), left.blocks()->map(prober));
    }

    shared_ptr<Block> HashMultiJoin::probe(const shared_ptr<Block> &left_block) {
        auto leftkeys = left_block->col(left_key_index_);
        auto leftrows = left_block->rows();
        auto block_size = left_block->size();

        auto output_block = make_shared<MemFlexBlock>(builder_->outputColOffset());

        for (uint32_t i = 0; i < block_size; ++i) {
            auto key = leftkeys->next().asInt();
            auto found = container_.find(key);
            if (found != container_.cend()) {
                DataRow &left_row = (*leftrows)[leftkeys->pos()];
                auto &exists = found->second;
                for (auto &right_row: *exists) {
                    builder_->build(output_block->push_back(), left_row, *right_row, key);
                }
            }
        }

        return output_block;
    }

    namespace powerjoin {

        PowerHashBasedJoin::PowerHashBasedJoin(function<int64_t(DataRow &)> left_key_maker,
                                               function<int64_t(DataRow &)> right_key_maker,
                                               lqf::JoinBuilder *builder, uint32_t expect_size,
                                               function<bool(DataRow &, DataRow &)> predicate) :
                left_key_maker_(left_key_maker), right_key_maker_(right_key_maker),
                builder_(unique_ptr<JoinBuilder>(builder)), predicate_(predicate), expect_size_(expect_size) {}

        shared_ptr<Table> PowerHashBasedJoin::join(Table &left, Table &right) {
            builder_->on(left, right);
            builder_->init();
            container_ = HashBuilder::buildContainer(right, right_key_maker_, builder_->snapshoter(), expect_size_);
            auto memTable = MemTable::Make(builder_->outputColSize(), builder_->useVertical());
            function<void(const shared_ptr<Block> &)> prober = bind(&PowerHashBasedJoin::probe, this,
                                                                    memTable.get(),
                                                                    _1);
            left.blocks()->foreach(prober);
            return memTable;
        }

        PowerHashJoin::PowerHashJoin(function<int64_t(DataRow &)> lkm, function<int64_t(DataRow &)> rkm,
                                     lqf::RowBuilder *rowBuilder, uint32_t expect_size,
                                     function<bool(DataRow &, DataRow &)> pred)
                : PowerHashBasedJoin(lkm, rkm, rowBuilder, expect_size, pred),
                  rowBuilder_(rowBuilder) {}

        void PowerHashJoin::probe(MemTable *output, const shared_ptr<Block> &leftBlock) {
            auto resultblock = output->allocate(leftBlock->size());

            auto leftrows = leftBlock->rows();
            uint32_t counter = 0;
            auto writer = resultblock->rows();
            auto left_block_size = leftBlock->size();
            if (predicate_) {
                if (outer_) {
                    for (uint32_t i = 0; i < left_block_size; ++i) {
                        DataRow &leftrow = leftrows->next();
                        auto leftval = left_key_maker_(leftrow);
                        auto result = container_->get(leftval);
                        DataRow &right = result ? *result : MemDataRow::EMPTY;
                        if (predicate_(leftrow, right)) {
                            rowBuilder_->build((*writer)[counter++], leftrow, right, leftval);
                        }
                    }
                } else {
                    for (uint32_t i = 0; i < left_block_size; ++i) {
                        DataRow &leftrow = leftrows->next();
                        auto leftval = left_key_maker_(leftrow);
                        auto result = container_->get(leftval);
                        if (result && predicate_(leftrow, *result)) {
                            rowBuilder_->build((*writer)[counter++], leftrow, *result, leftval);
                        }
                    }
                }
            } else {
                if (outer_) {
                    for (uint32_t i = 0; i < left_block_size; ++i) {
                        DataRow &leftrow = leftrows->next();
                        auto leftval = left_key_maker_(leftrow);
                        auto result = container_->get(leftval);
                        rowBuilder_->build((*writer)[counter++], leftrow, *result, leftval);
                    }
                } else {
                    for (uint32_t i = 0; i < left_block_size; ++i) {
                        DataRow &leftrow = leftrows->next();
                        auto leftval = left_key_maker_(leftrow);
                        auto result = container_->get(leftval);
                        if (result) {
                            rowBuilder_->build((*writer)[counter++], leftrow, *result, leftval);
                        }
                    }
                }
            }
            resultblock->resize(counter);
        }

        PowerHashColumnJoin::PowerHashColumnJoin(function<int64_t(DataRow &)> lkm, function<int64_t(DataRow &)> rkm,
                                                 lqf::ColumnBuilder *colBuilder)
                : PowerHashBasedJoin(lkm, rkm, colBuilder), columnBuilder_(colBuilder) {}


        void PowerHashColumnJoin::probe(lqf::MemTable *owner, const shared_ptr<Block> &leftBlock) {
            shared_ptr<MemvBlock> leftvBlock = static_pointer_cast<MemvBlock>(leftBlock);
            /// Make sure the cast is valid
            assert(leftvBlock.get() != nullptr);

            auto leftrows = leftBlock->rows();

            MemvBlock vblock(leftBlock->size(), columnBuilder_->rightColSize());
            auto writer = vblock.rows();
            auto left_block_size = leftBlock->size();
            for (uint32_t i = 0; i < left_block_size; ++i) {
                DataRow &leftrow = leftrows->next();
                auto leftkey = left_key_maker_(leftrow);
                auto result = container_->get(leftkey);
                (*writer)[i] = *result;
            }

            auto newblock = owner->allocate(0);
            // Merge result block with original block
            auto newvblock = static_pointer_cast<MemvBlock>(newblock);

            columnBuilder_->build(*newvblock, *leftvBlock, vblock);
//            newvblock->merge(*leftvBlock, columnBuilder_->leftInst());
//            newvblock->merge(vblock, columnBuilder_->rightInst());
        }

        PowerHashFilterJoin::PowerHashFilterJoin(function<int64_t(DataRow &)> left_key_maker,
                                                 function<int64_t(DataRow &)> right_key_maker)
                : left_key_maker_(left_key_maker), right_key_maker_(right_key_maker) {}

        shared_ptr<Block> PowerHashFilterJoin::probe(const shared_ptr<Block> &leftBlock) {
            auto bitmap = make_shared<SimpleBitmap>(leftBlock->limit());
            auto rows = leftBlock->rows();
            auto left_block_size = leftBlock->size();
            if (anti_) {
                for (uint32_t i = 0; i < left_block_size; ++i) {
                    auto key = left_key_maker_(rows->next());
                    if (!predicate_->test(key)) {
                        bitmap->put(rows->pos());
                    }
                }
            } else {
                for (uint32_t i = 0; i < left_block_size; ++i) {
                    auto key = left_key_maker_(rows->next());
                    if (predicate_->test(key)) {
                        bitmap->put(rows->pos());
                    }
                }
            }
            return leftBlock->mask(bitmap);
        }

        shared_ptr<Table> PowerHashFilterJoin::join(Table &left, Table &right) {
            predicate_ = HashBuilder::buildHashPredicate(right, right_key_maker_);

            function<shared_ptr<Block>(const shared_ptr<Block> &)> prober = bind(&PowerHashFilterJoin::probe, this,
                                                                                 _1);
            return make_shared<TableView>(left.type(), left.colSize(), left.blocks()->map(prober));
        }

    }
}
