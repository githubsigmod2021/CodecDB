//
// Created by Harper on 6/15/20.
//

#include "data_container.h"

using namespace lqf;

namespace lqf {
    namespace datacontainer {

        MemRowVector::MemRowVector(const vector<uint32_t> &offset)
                : MemRowVector(offset, VECTOR_SLAB_SIZE_) {}

        MemRowVector::MemRowVector(const vector<uint32_t> &offset, uint32_t slab_size)
                : accessor_(offset), slab_size_(slab_size), stripe_offset_(0), size_(0) {
            memory_.push_back(make_shared<vector<uint64_t>>(slab_size_));
            row_size_ = offset.back();
            stripe_size_ = slab_size_ / row_size_;
        }

        DataRow &MemRowVector::push_back() {
            auto current = stripe_offset_;
            stripe_offset_ += row_size_;
            if (stripe_offset_ > slab_size_) {
                memory_.push_back(make_shared<vector<uint64_t>>(slab_size_));
                stripe_offset_ = row_size_;
                current = 0;
            }
            size_++;
            accessor_.raw(memory_.back()->data() + current);
            return accessor_;
        }

        DataRow &MemRowVector::operator[](uint32_t index) {
            auto stripe_index = index / stripe_size_;
            auto offset = index % stripe_size_;
            accessor_.raw(memory_[stripe_index]->data() + offset * row_size_);
            return accessor_;
        }

        class MemRowVectorIterator : public Iterator<DataRow &> {
        protected:
            vector<shared_ptr<vector<uint64_t>>> &memory_ref_;
            uint32_t index_;
            uint32_t pointer_;
            uint32_t counter_;
            uint32_t size_;
            uint32_t row_size_;
            MemDataRowPointer accessor_;
        public:
            MemRowVectorIterator(vector<shared_ptr<vector<uint64_t>>> &ref,
                                 const vector<uint32_t> &offset, uint32_t size)
                    : memory_ref_(ref), index_(0), pointer_(0), counter_(0),
                      size_(size), row_size_(offset.back()), accessor_(offset) {}

            bool hasNext() override {
                return counter_ < size_;
            }

            DataRow &next() override {
                accessor_.raw(memory_ref_[index_]->data() + pointer_);
                pointer_ += row_size_;
                if (pointer_ > memory_ref_[index_]->size()) {
                    ++index_;
                    pointer_ = 0;
                    accessor_.raw(memory_ref_[index_]->data() + pointer_);
                    pointer_ += row_size_;
                }
                ++counter_;
                return accessor_;
            }
        };

        unique_ptr<Iterator<DataRow &>> MemRowVector::iterator() {
            return unique_ptr<Iterator<DataRow &>>(new MemRowVectorIterator(memory_, accessor_.offset(), size_));
        }

        MemRowMap::MemRowMap(const vector<uint32_t> &offset) : MemRowVector(offset) {}

        MemRowMap::MemRowMap(const vector<uint32_t> &offset, uint32_t slab_size)
                : MemRowVector(offset, slab_size) {}

        DataRow &MemRowMap::insert(uint64_t key) {
            push_back();
            uint64_t anchor = (static_cast<uint64_t>(memory_.size() - 1) << 32) | (stripe_offset_ - row_size_);
            map_[key] = anchor;
            return accessor_;
        }

        DataRow &MemRowMap::operator[](uint64_t key) {
            auto found = map_.find(key);
            if (found == map_.end()) {
                return insert(key);
            }
            auto anchor = found->second;
            auto index = static_cast<uint32_t>(anchor >> 32);
            auto offset = static_cast<uint32_t>(anchor);
            accessor_.raw(memory_[index]->data() + offset);
            return accessor_;
        }

        DataRow *MemRowMap::find(uint64_t key) {
            auto found = map_.find(key);
            if (found == map_.end()) {
                return nullptr;
            }
            auto anchor = found->second;
            auto index = static_cast<uint32_t>(anchor >> 32);
            auto offset = static_cast<uint32_t>(anchor);
            accessor_.raw(memory_[index]->data() + offset);
            return &accessor_;
        }

        class MemRowMapIterator : public Iterator<pair<uint64_t, DataRow &> &> {
        protected:
            vector<shared_ptr<vector<uint64_t>>> &memory_ref_;
            unordered_map<uint64_t, uint64_t>::iterator map_it_;
            unordered_map<uint64_t, uint64_t>::const_iterator map_end_;
            MemDataRowPointer accessor_;
            pair<uint64_t, DataRow &> pair_;
        public:
            MemRowMapIterator(vector<shared_ptr<vector<uint64_t>>> &ref,
                              const vector<uint32_t> &offset, unordered_map<uint64_t, uint64_t> &map)
                    : memory_ref_(ref), map_it_(map.begin()), map_end_(map.cend()), accessor_(offset),
                      pair_(0, accessor_) {}

            bool hasNext() override {
                return map_it_ != map_end_;
            }

            pair<uint64_t, DataRow &> &next() override {
                pair_.first = map_it_->first;
                auto anchor = map_it_->second;
                auto index = static_cast<uint32_t>(anchor >> 32);
                auto pointer = static_cast<uint32_t>(anchor);
                accessor_.raw(memory_ref_[index]->data() + pointer);
                map_it_++;
                return pair_;
            }
        };

        unique_ptr<Iterator<pair<uint64_t, DataRow &> &>> MemRowMap::map_iterator() {
            return unique_ptr<Iterator<pair<uint64_t, DataRow &> &>>(
                    new MemRowMapIterator(memory_, accessor_.offset(), map_));
        }

        template<typename KEY, typename MAP>
        CMemRowMap<KEY, MAP>::CMemRowMap(uint32_t expect_size, const vector<uint32_t> &col_offset)
                : CMemRowMap(expect_size, col_offset, CMAP_SLAB_SIZE_) {}


        template<typename KEY, typename MAP>
        CMemRowMap<KEY, MAP>::CMemRowMap(uint32_t expect_size, const vector<uint32_t> &col_offset, uint32_t slab_size)
                : anchor_([&col_offset, slab_size]() {
            MapAnchor anchor{0, slab_size, MemDataRowPointer(col_offset)};
            return anchor;
        }), map_(expect_size), memory_(1000), slab_size_(slab_size), col_offset_(col_offset), memory_watermark_(0),
                  row_size_(col_offset.back()) {}

        template<typename KEY, typename MAP>
        void CMemRowMap<KEY, MAP>::new_slab() {
            auto anchor = anchor_.get();
            memory_lock_.lock();
            anchor->index_ = memory_watermark_;
            memory_[memory_watermark_++] = make_shared<vector<uint64_t>>(slab_size_);
            anchor->offset_ = 0;
            memory_lock_.unlock();
        }

        template<typename KEY, typename MAP>
        DataRow &CMemRowMap<KEY, MAP>::insert(KEY key) {
            auto anchor = anchor_.get();
            if (anchor->offset_ + row_size_ > slab_size_) {
                new_slab();
            }
            if(memory_[anchor->index_]== NULL) {
                return anchor->accessor_;
            }
            anchor->accessor_.raw(memory_[anchor->index_]->data() + anchor->offset_);
            map_.put(key, (anchor->index_ << 20) | anchor->offset_);
            anchor->offset_ += row_size_;
            return anchor->accessor_;
        }

        template<typename KEY, typename MAP>
        DataRow &CMemRowMap<KEY, MAP>::operator[](KEY key) {
            int pos = map_.get(key);
            if (pos == INT32_MIN) {
                return insert(key);
            }
            auto anchor = anchor_.get();
            int index = pos >> 20;
            int offset = pos & 0xFFFFF;
            anchor->accessor_.raw(memory_[index]->data() + offset);
            return anchor->accessor_;
        }

        template<typename KEY, typename MAP>
        DataRow *CMemRowMap<KEY, MAP>::find(KEY key) {
            int pos = map_.get(key);
            if (pos == INT32_MIN) {
                return nullptr;
            }
            auto anchor = anchor_.get();
            int index = pos >> 20;
            int offset = pos & 0xFFFFF;
            anchor->accessor_.raw(memory_[index]->data() + offset);
            return &(anchor->accessor_);
        }

        template<typename KEY, typename MAP>
        bool CMemRowMap<KEY, MAP>::test(KEY key) {
            return map_.get(key) != INT32_MIN;
        }

        template<typename KEY, typename MAP>
        DataRow *CMemRowMap<KEY, MAP>::remove(KEY key) {
            int pos = map_.remove(key);
            if (pos == INT32_MIN) {
                return nullptr;
            }
            auto anchor = anchor_.get();
            int index = pos >> 20;
            int offset = pos & 0xFFFFF;
            anchor->accessor_.raw(memory_[index]->data() + offset);
            return &(anchor->accessor_);
        }

        template<typename KEY, typename MAP>
        uint32_t CMemRowMap<KEY, MAP>::size() {
            return map_.size();
        }

        template<typename KEY>
        class CMemRowMapIterator : public Iterator<pair<KEY, DataRow &> &> {
        protected:
            vector<shared_ptr<vector<uint64_t>>> &memory_ref_;
            unique_ptr<Iterator<pair<KEY, int32_t>>> map_iterator_;
            MemDataRowPointer accessor_;
            pair<KEY, DataRow &> pair_;
        public:
            CMemRowMapIterator(vector<shared_ptr<vector<uint64_t>>> &ref,
                               const vector<uint32_t> &offset, unique_ptr<Iterator<pair<KEY, int32_t>>> map_iterator)
                    : memory_ref_(ref), map_iterator_(move(map_iterator)), accessor_(offset), pair_(0, accessor_) {}

            bool hasNext() override {
                return map_iterator_->hasNext();
            }

            pair<KEY, DataRow &> &next() override {
                auto entry = map_iterator_->next();
                auto anchor = entry.second;
                auto index = anchor >> 20;
                auto pointer = anchor & 0xFFFFF;
                accessor_.raw(memory_ref_[index]->data() + pointer);
                pair_.first = entry.first;
                return pair_;
            }
        };

        template<typename KEY, typename MAP>
        unique_ptr<Iterator<pair<KEY, DataRow &> &> > CMemRowMap<KEY, MAP>::map_iterator() {
            return unique_ptr<Iterator<pair<KEY, DataRow &> &>>(
                    new CMemRowMapIterator<KEY>(memory_, col_offset_, map_.iterator()));
        }
    }
}