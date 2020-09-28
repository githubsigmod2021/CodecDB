//
// Created by harper on 2/27/20.
//

#ifndef ARROW_HEAP_H
#define ARROW_HEAP_H

#include <vector>
#include <functional>
#include <algorithm>
#include <cstdint>

namespace lqf {
    using namespace std;

    template<typename ELEM_PNT>
    class Heap {
    private:
        uint32_t size_;
        vector<ELEM_PNT> data_;
        function<ELEM_PNT()> creator_;
        function<bool(ELEM_PNT, ELEM_PNT)> comparator_;
    public:
        Heap(uint32_t size, function<ELEM_PNT()> creator, function<bool(ELEM_PNT, ELEM_PNT)> comp)
                : size_(size), data_(), creator_(creator), comparator_(comp) {
        }

        ~Heap() {
            for (auto &d:data_) {
                delete d;
            }
        };

        vector<ELEM_PNT> &content() {
            return data_;
        }

        void add(ELEM_PNT element) {
            if (__builtin_expect(data_.size() < size_, 0)) {
                ELEM_PNT latest = creator_();
                *latest = *element;
                data_.push_back(latest);
            } else if (comparator_(element, data_[0])) {
                *(data_[0]) = *element;
                adjust(0);
            }
        }

        void done() {
            std::sort(data_.begin(), data_.end(), comparator_);
        }

        void heapify() {
            int start = (data_.size() - 1) / 2;
            while (start >= 0) {
                adjust(start--);
            }
        }

    protected:
        void adjust(int index) {
            int limit = (data_.size() - 3) / 2;
            int pointer = index;
            while (pointer <= limit) {
                int left = pointer * 2 + 1;
                int right = pointer * 2 + 2;
                int largerChild = comparator_(data_[left], data_[right]) ? right : left;
                if (comparator_(data_[pointer], data_[largerChild])) {
                    swap(pointer, largerChild);
                    pointer = largerChild;
                } else {
                    break;
                }
            }
            if (pointer == limit + 1 && data_.size() % 2 == 0) {
                int parent = (data_.size() - 1) / 2;
                int child = data_.size() - 1;
                if (comparator_(data_[parent], data_[child])) {
                    swap(parent, child);
                }
            }
        }

        void swap(int a, int b) {
            ELEM_PNT temp = data_[a];
            data_[a] = data_[b];
            data_[b] = temp;
        }
    };
}

#endif //ARROW_HEAP_H
