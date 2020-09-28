//
// Created by harper on 3/17/20.
//

#include "mat.h"
#include "join.h"

using namespace std::placeholders;

namespace lqf {

    FilterMat::FilterMat() : Node(1) {}

    unique_ptr<NodeOutput> FilterMat::execute(const vector<NodeOutput *> &inputs) {
        auto input0 = static_cast<TableOutput *>(inputs[0]);
        auto table = mat(*(input0->get()));
        return unique_ptr<TableOutput>(new TableOutput(table));
    }

    shared_ptr<Table> FilterMat::mat(Table &input) {
        // Instead of using a hashmap and involves concurrency problem, use an array instead.
        vector<shared_ptr<Bitmap>> storage(100, nullptr);
        ParquetTable *owner;
        function<void(const shared_ptr<Block> &)> processor = [&storage, &owner](const shared_ptr<Block> &block) {
            auto mblock = dynamic_pointer_cast<MaskedBlock>(block);
            owner = static_cast<ParquetTable *>(mblock->inner()->owner());
            storage[mblock->inner()->id()] = mblock->mask();
        };
        input.blocks()->foreach(processor);

        return make_shared<MaskedTable>(owner, storage);
    }

    HashMat::HashMat(uint32_t key_index, unique_ptr<Snapshoter> snapshoter, uint32_t expect_size)
            : Node(1), key_index_(key_index), snapshoter_(move(snapshoter)), expect_size_(expect_size) {}

    unique_ptr<NodeOutput> HashMat::execute(const vector<NodeOutput *> &inputs) {
        auto input0 = static_cast<TableOutput *>(inputs[0]);
        auto table = mat(*(input0->get()));
        return unique_ptr<TableOutput>(new TableOutput(table));
    }

    shared_ptr<Table> HashMat::mat(Table &input) {
        if (snapshoter_) {
            // Make Container
            auto table = MemTable::Make(offset2size(snapshoter_->colOffset()));
            auto container = HashBuilder::buildContainer(input, key_index_, snapshoter_.get(), expect_size_);
            auto block = make_shared<HashMemBlock<Hash32Container>>(move(container));
            table->append(block);
            return table;
        } else {
            auto table = MemTable::Make(colSize(0));
            auto predicate = HashBuilder::buildHashPredicate(input, key_index_, expect_size_);
            auto block = make_shared<HashMemBlock<IntPredicate<Int32>>>(move(predicate));
            table->append(block);
            return table;
        }
    }

    PowerHashMat::PowerHashMat(function<int64_t(DataRow &)> key_maker,
                               unique_ptr<Snapshoter> snapshoter)
            : Node(1), key_maker_(key_maker), snapshoter_(move(snapshoter)) {}

    unique_ptr<NodeOutput> PowerHashMat::execute(const vector<NodeOutput *> &inputs) {
        auto input0 = static_cast<TableOutput *>(inputs[0]);
        auto table = mat(*(input0->get()));
        return unique_ptr<TableOutput>(new TableOutput(table));
    }

    shared_ptr<Table> PowerHashMat::mat(Table &input) {
        auto table = MemTable::Make(offset2size(snapshoter_->colOffset()));
        if (snapshoter_) {
            // Make Container
            auto container = HashBuilder::buildContainer(input, key_maker_, snapshoter_.get());
            auto block = make_shared<HashMemBlock<Hash64Container>>(move(container));
            table->append(block);
        } else {
            auto predicate = HashBuilder::buildHashPredicate(input, key_maker_);
            auto block = make_shared<HashMemBlock<IntPredicate<Int64>>>(move(predicate));
            table->append(block);
        }
        return table;
    }
}