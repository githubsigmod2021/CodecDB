//
// Created by harper on 3/22/20.
//

#include "filter_executor.h"

namespace lqf {

    using namespace sboost;

    unique_ptr<FilterExecutor> FilterExecutor::inst = unique_ptr<FilterExecutor>(new FilterExecutor());

    FilterExecutor::FilterExecutor() {}

    void FilterExecutor::reg(Table &table, ColPredicate &predicate) {
        auto key = makeKey(table, predicate.index());
        lock_guard<mutex> lock(write_lock);
        auto place = regTable_.emplace(key, new vector<ColPredicate *>());
        (*place.first).second->push_back(&predicate);
    }

    shared_ptr<Bitmap> FilterExecutor::executeSimple(Block &block, Bitmap &skip, SimplePredicate &predicate) {
        return predicate.filterBlock(block, skip);
    }

    void FilterExecutor::reset() {
        regTable_.clear();
        result_.clear();
    }

    using namespace lqf::sboost;

    template<typename DTYPE>
    shared_ptr<Bitmap> FilterExecutor::executeSboost(Block &block, SboostPredicate<DTYPE> &predicate) {

        ParquetBlock *ppblock;
        MaskedBlock *pmblock = dynamic_cast<MaskedBlock *>(&block);
        if (pmblock) {
            ppblock = static_cast<ParquetBlock *>(pmblock->inner().get());
        } else {
            ppblock = static_cast<ParquetBlock *>(&block);
        }
        auto mappingKey = makeKey(*(ppblock->owner()), predicate.index());

        stringstream ss;
        ss << mappingKey << "." << ppblock->index();
        auto resultKey = ss.str();

        unique_lock<mutex> result_key_lock(write_lock);
        auto result = result_locks_.emplace(resultKey, make_shared<mutex>());
        auto result_lock = result.first->second;
        result_key_lock.unlock();

        lock_guard<mutex> lock_for_result_exec(*result_lock);
        auto found = result_.find(resultKey);
        if (found != result_.end()) {
            return (*(found->second))[&predicate];
        }
        auto ite = regTable_.find(mappingKey);
        if (ite != regTable_.end()) {
            auto preds = *(ite->second).get();
            vector<unique_ptr<RawAccessor<DTYPE>>> content;

            for (auto pred: preds) {
                auto spred = dynamic_cast<SboostPredicate<DTYPE> *>(pred);
                content.push_back(spred->build());
            }

            PackedRawAccessor<DTYPE> packedAccessor(content);

            ppblock->raw(predicate.index(), &packedAccessor);

            auto resultMap = new unordered_map<ColPredicate *, shared_ptr<Bitmap>>();
            for (uint32_t i = 0; i < preds.size(); ++i) {
                (*resultMap)[preds[i]] = move(content[i]->result());
            }

            unique_lock<mutex> lock(write_lock);
            result_[resultKey] = unique_ptr<unordered_map<ColPredicate *, shared_ptr<Bitmap>>>(resultMap);
            lock.unlock();

            // Here we do not bitand the result with mask, as this will be done in Filter::processBlock
            return (*resultMap)[&predicate];
        }
        return nullptr;
    }

    string FilterExecutor::makeKey(Table &table, uint32_t index) {
        stringstream ss;
        ss << (uint64_t) &table << "." << index;
        return ss.str();
    }

    template<typename DTYPE>
    PackedRawAccessor<DTYPE>::PackedRawAccessor(vector<unique_ptr<RawAccessor<DTYPE>>> &content)
            :content_(content) {}

    template<typename DTYPE>
    void PackedRawAccessor<DTYPE>::init(uint64_t size) {
        for (auto const &item: content_) {
            item->init(size);
        }
    }

    template<typename DTYPE>
    void PackedRawAccessor<DTYPE>::dict(Dictionary<DTYPE> &dict) {
        for (auto const &item: content_) {
            item->dict(dict);
        }
    }

    template<typename DTYPE>
    void PackedRawAccessor<DTYPE>::data(DataPage *dpage) {
        for (auto const &item: content_) {
            item->data(dpage);
        }
    }

    template shared_ptr<Bitmap>
    FilterExecutor::executeSboost<Int32Type>(Block &block, SboostPredicate<Int32Type> &predicate);

    template shared_ptr<Bitmap>
    FilterExecutor::executeSboost<ByteArrayType>(Block &block, SboostPredicate<ByteArrayType> &predicate);

    template shared_ptr<Bitmap>
    FilterExecutor::executeSboost<DoubleType>(Block &block, SboostPredicate<DoubleType> &predicate);
}