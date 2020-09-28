//
// Created by harper on 4/8/20.
//

#include "union.h"

namespace lqf {

    FilterUnion::FilterUnion(uint32_t num_input) : Node(num_input) {}

    unique_ptr<NodeOutput> FilterUnion::execute(const vector<NodeOutput *> &inputs) {
        vector<Table *> inputTables;
        for (auto &input: inputs) {
            inputTables.push_back(static_cast<TableOutput *>(input)->get().get());
        }
        return unique_ptr<TableOutput>(new TableOutput(execute(inputTables)));
    }

    shared_ptr<Table> FilterUnion::execute(const vector<Table *> &tables) {
        vector<shared_ptr<Bitmap>> storage(100, nullptr);
        ParquetTable *owner;
        function<void(const shared_ptr<Block> &)> processor = [&storage, &owner](
                const shared_ptr<Block> &block) {
            auto mblock = dynamic_pointer_cast<MaskedBlock>(block);
            owner = static_cast<ParquetTable *>(mblock->inner()->owner());
            auto blockid = mblock->inner()->id();

            if (storage[blockid]) {
                *(storage[blockid]) | (*mblock->mask());
            } else {
                storage[blockid] = mblock->mask();
            }
        };
        for (auto &table: tables) {
            table->blocks()->foreach(processor);
        }
        return make_shared<MaskedTable>(owner, storage);
    }

    FilterAnd::FilterAnd(uint32_t num_input) : Node(num_input) {}

    unique_ptr<NodeOutput> FilterAnd::execute(const vector<NodeOutput *> &inputs) {
        vector<Table *> inputTables;
        for (auto &input: inputs) {
            inputTables.push_back(static_cast<TableOutput *>(input)->get().get());
        }
        return unique_ptr<TableOutput>(new TableOutput(execute(inputTables)));
    }

    shared_ptr<Table> FilterAnd::execute(const vector<Table *> &tables) {
        vector<shared_ptr<Bitmap>> storage(100, nullptr);
        ParquetTable *owner;
        function<void(const shared_ptr<Block> &)> processor = [&storage, &owner](
                const shared_ptr<Block> &block) {
            auto mblock = dynamic_pointer_cast<MaskedBlock>(block);
            owner = static_cast<ParquetTable *>(mblock->inner()->owner());
            auto blockid = mblock->inner()->id();

            if (storage[blockid]) {
                *(storage[blockid]) & (*mblock->mask());
            } else {
                storage[blockid] = mblock->mask();
            }
        };
        for (auto &table: tables) {
            table->blocks()->foreach(processor);
        }

        return make_shared<MaskedTable>(owner, storage);
    }
}