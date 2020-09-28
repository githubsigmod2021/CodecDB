//
// Created by harper on 4/11/20.
//

#include <parquet/encoding.h>
#include "dict.h"
#include "memorypool.h"

namespace lqf {

    using namespace parquet;
    using namespace lqf::memory;

    template<typename DTYPE>
    Dictionary<DTYPE>::Dictionary() {

    }

    template<typename DTYPE>
    Dictionary<DTYPE>::Dictionary(shared_ptr<DictionaryPage> dpage) {
        auto decoder = parquet::MakeTypedDecoder<DTYPE>(Encoding::PLAIN, nullptr);
        size_ = dpage->num_values();
        decoder->SetData(size_, dpage->data(), dpage->size());
        buffer_ = (T *) malloc(sizeof(T) * size_);
        auto loaded = decoder->Decode(buffer_, size_);
        // TODO Check why the following code cause error
//        if (sizeof(T) == 16) { // This is a byte array, need to copy to mempool
//            for (auto i = 0u; i < size_; ++i) {
//                ByteArray *ba = (ByteArray *) buffer_ + i;
//                ByteArrayBuffer::instance.allocate(*ba);
//            }
//        }
    }

    template<typename DTYPE>
    Dictionary<DTYPE>::~Dictionary() {
        if (nullptr != buffer_)
            free(buffer_);
    }

    template<typename DTYPE>
    const typename DTYPE::c_type &Dictionary<DTYPE>::operator[](int32_t key) {
        return buffer_[key];
    }

    template<typename DTYPE>
    int32_t Dictionary<DTYPE>::lookup(const T &key) {
        uint32_t low = 0;
        uint32_t high = size_;

        while (low <= high && low < size_) {
            uint32_t mid = (low + high) >> 1;
            T midVal = buffer_[mid];

            if (midVal < key)
                low = mid + 1;
            else if (midVal > key)
                high = mid - 1;
            else
                return mid; // key found
        }
        return -(low + 1);  // key not found.
    }

    template<typename DTYPE>
    unique_ptr<vector<uint32_t>> Dictionary<DTYPE>::list(function<bool(const T &)> pred) {
        unique_ptr<vector<uint32_t>> result = unique_ptr<vector<uint32_t>>(new vector<uint32_t>());
        for (uint32_t i = 0; i < size_; ++i) {
            if (pred(buffer_[i])) {
                result->push_back(i);
            }
        }
        return result;
    }

    template
    class Dictionary<Int32Type>;

    template
    class Dictionary<DoubleType>;

    template
    class Dictionary<ByteArrayType>;

}