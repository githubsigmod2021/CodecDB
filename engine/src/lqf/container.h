//
// Created by Harper on 4/20/20.
//

#ifndef ARROW_CONTAINER_H
#define ARROW_CONTAINER_H

#include <cstdint>
#include <vector>
#include <memory>
#include <cstring>
#include <mutex>
#include <fstream>
#include <atomic>
#include "lang.h"
#include "hash.h"

#define SCALE 1.5

using namespace std;

namespace lqf {

    using namespace lqf::hash;
    namespace container {

        uint32_t ceil2(uint32_t);

        template<typename DTYPE>
        class PhaseConcurrentHashSet {
            using type = typename DTYPE::type;
        protected:
            std::atomic<uint32_t> size_;

            vector<type> *content_;

            bool _insert(vector<type> *content, type value) {
                auto data = content->data();
                auto content_size = content_->size();
                auto content_mask = content_size - 1;
                auto insert_index = knuth_hash(value) & content_mask;
                auto insert_value = value;
                while (insert_value != DTYPE::empty) {
                    auto exist = (*content)[insert_index];
                    if (exist == insert_value) {
                        return 0;
                    } else if (exist > insert_value) {
                        insert_index = (insert_index + 1) & content_mask;
                    } else if (__sync_bool_compare_and_swap(data + insert_index, exist, insert_value)) {
                        insert_value = exist;
                        insert_index = (insert_index + 1) & content_mask;
                    }
                }
                return 1;
            }

            pair<uint32_t, type> _findreplacement(uint32_t start_index) {
                auto content_mask = content_->size() - 1;
                auto ref_index = start_index;
                auto data = content_->data();
                type ref_value;
                do {
                    ++ref_index;
                    ref_value = data[ref_index & content_mask];
                } while (ref_value != DTYPE::empty && (knuth_hash(ref_value) & content_mask) > start_index);
                auto back_index = ref_index - 1;
                while (back_index > start_index) {
                    auto cand_value = data[back_index & content_mask];
                    if (cand_value == DTYPE::empty || (knuth_hash(cand_value) & content_mask) <= start_index) {
                        ref_value = cand_value;
                        ref_index = back_index;
                    }
                    --back_index;
                }
                return pair<uint32_t, type>(ref_index, ref_value);
            }

        public:
            PhaseConcurrentHashSet() : PhaseConcurrentHashSet(1048576) {};

            PhaseConcurrentHashSet(uint32_t expect_size) : size_(0) {
                content_ = new vector<type>(ceil2(expect_size * SCALE), DTYPE::empty);
            }

            virtual ~PhaseConcurrentHashSet() {
                delete content_;
            }

            PhaseConcurrentHashSet(PhaseConcurrentHashSet &) = delete;

            PhaseConcurrentHashSet(PhaseConcurrentHashSet &&) = delete;

            PhaseConcurrentHashSet &operator=(PhaseConcurrentHashSet &) = delete;

            PhaseConcurrentHashSet &operator=(PhaseConcurrentHashSet &&) = delete;

            void add(type value) {
                if (_insert(content_, value)) {
                    size_++;
                    if (size_ >= limit()) {
                        // TODO What error?
                        throw std::invalid_argument("hash set is full");
                    }
                }
                // resize operation cannot be done here.
                // Will race with insert
//            if (size_ * SCALE > content_->size()) {
//                internal_resize(content_->size() << 1);
//            }
            }

            bool test(type value) {
                auto content_size = content_->size();
                auto content_mask = content_size - 1;
                auto index = knuth_hash(value) & content_mask;
                while ((*content_)[index] != DTYPE::empty && (*content_)[index] != value) {
                    index = (index + 1) & content_mask;
                }
                return (*content_)[index] == value;
            }

            void remove(type value) {
                auto data = content_->data();
                auto content_mask = content_->size() - 1;
                int start_index = knuth_hash(value) & content_mask;
                int ref_index = start_index;
                auto ref_value = value;
                while (data[ref_index & content_mask] != DTYPE::empty
                       && data[ref_index & content_mask] > ref_value) {
                    ++ref_index;
                }
                // The expecting key is found iff at least one cas happens
                bool cas_success = false;
                while (ref_index >= start_index) {
                    if (ref_value == DTYPE::empty || data[ref_index & content_mask] != ref_value) {
                        --ref_index;
                    } else {
                        auto cand = _findreplacement(ref_index);
                        auto cand_index = cand.first;
                        auto cand_value = cand.second;
                        auto data_index = ref_index & content_mask;
                        if (__sync_bool_compare_and_swap(data + data_index, ref_value, cand_value)) {
                            cas_success = true;
                            if (cand_value != DTYPE::empty) {
                                ref_index = cand_index;
                                ref_value = cand_value;
                                start_index = knuth_hash(ref_value) & content_mask;
                            } else {
                                if (cas_success) {
                                    size_--;
                                }
                                return;
                            }
                        } else {
                            --ref_index;
                        }
                    }
                }
                if (cas_success) {
                    size_--;
                }
            }

            void resize(uint32_t expect) {
                auto new_size = ceil2(expect);
                if (content_->size() < new_size) {
                    auto new_content = new vector<type>(new_size, DTYPE::empty);
                    // Rehash all elements
                    for (auto &value: *content_) {
                        if (value != DTYPE::empty) {
                            _insert(new_content, value);
                        }
                    }

                    delete content_;
                    content_ = new_content;
                }
            }

            class PCHSIterator : public Iterator<typename DTYPE::type> {
                using type = typename DTYPE::type;
            protected:
                vector<type> &content_;
                uint32_t pointer_;
            public:
                PCHSIterator(vector<type> &content) : content_(content), pointer_(0) {
                    while (pointer_ < content_.size() && content_[pointer_] == DTYPE::empty) {
                        ++pointer_;
                    }
                }

                bool hasNext() override {
                    return pointer_ < content_.size();
                }

                type next() override {
                    type value = content_[pointer_];
                    do { ++pointer_; } while (pointer_ < content_.size() && content_[pointer_] == DTYPE::empty);
                    return value;
                }
            };

            unique_ptr<Iterator<type>> iterator() {
                return unique_ptr<PCHSIterator>(new PCHSIterator(*content_));
            }

            inline uint32_t size() { return size_; }

            inline uint32_t limit() { return (*content_).size(); }
        };

        class PhaseConcurrentIntHashMap {
        protected:
            std::atomic<uint32_t> size_;

            vector<uint64_t> content_;
            uint32_t content_len_;

            bool _insert(vector<uint64_t> *content, uint32_t content_len, uint64_t entry);

            pair<uint32_t, uint64_t> _findreplacement(uint32_t start_index);

        public:

            PhaseConcurrentIntHashMap();

            PhaseConcurrentIntHashMap(uint32_t expect_size);

            void put(int key, int value);

            int get(int key);

            int remove(int key);

            void resize(uint64_t expect);

            unique_ptr<Iterator<pair<int, int>>> iterator();

            inline uint32_t size() { return size_; }

            inline uint32_t limit() { return content_len_; }
        };

        inline __uint128_t cas128(volatile __uint128_t *src, __uint128_t cmp, __uint128_t exchange) {
            bool success;
            __asm__ __volatile__ (
            "lock cmpxchg16b %1"
            : "+A" ( cmp ), "+m" ( *src ), "=@ccz"(success)
            : "b" ((uint64_t) exchange), "c" ((uint64_t) (exchange >> 64))
            : "cc"
            );
            return success;
        }

        struct Pair64 {
            int64_t key_;
            int32_t value_;
        };

        union Entry64 {
            Pair64 pair;
            __uint128_t whole;
        };

        class PhaseConcurrentInt64HashMap {
        protected:
            std::atomic<uint32_t> size_;

            Entry64 *content_;
            uint32_t content_len_;

            bool _insert(Entry64 *content, uint32_t content_len, Entry64 entry);

            pair<uint32_t, Entry64> _findreplacement(uint32_t start_index);

        public:

            PhaseConcurrentInt64HashMap();

            PhaseConcurrentInt64HashMap(uint32_t expect_size);

            virtual ~PhaseConcurrentInt64HashMap();

            PhaseConcurrentInt64HashMap(PhaseConcurrentInt64HashMap &) = delete;

            PhaseConcurrentInt64HashMap(PhaseConcurrentInt64HashMap &&) = delete;

            PhaseConcurrentInt64HashMap &operator=(PhaseConcurrentInt64HashMap &) = delete;

            PhaseConcurrentInt64HashMap &operator=(PhaseConcurrentInt64HashMap &&) = delete;

            void put(int64_t key, int32_t value);

            int32_t get(int64_t key);

            int32_t remove(int64_t key);

            void resize(uint64_t expect);

            unique_ptr<Iterator<pair<int64_t, int32_t>>> iterator();

            inline uint32_t size() { return size_; }

            inline uint32_t limit() { return content_len_; }
        };

        template<typename KTYPE, typename VTYPEP>
        class PhaseConcurrentHashMap {
            using ktype = typename KTYPE::type;
        protected:
            struct Pair {
                ktype key_;
                VTYPEP value_;
            };

            union Entry {
                Pair pair;
                __uint128_t whole;
            };

            std::atomic<uint32_t> size_;

            Entry *content_;
            uint32_t content_len_;

            bool _insert(Entry *content, uint32_t content_len, Entry entry) {
                auto data = content;
                auto content_mask = content_len - 1;
                auto insert_index = knuth_hash(entry.pair.key_) & content_mask;

                auto insert_value = entry;

                while (insert_value.pair.key_ != KTYPE::empty) {
                    auto exist = content[insert_index];

                    if (exist.pair.key_ == insert_value.pair.key_) {
                        // We do not copy the data here as in a parallel env it is not clear who goes first.
                        // Copying data here may cause a data race and need either a CAS or a lock.
                        // As we can always assume the exist one goes later, not copying the data is also not wrong.
                        return 0;
                    } else if (exist.pair.key_ > insert_value.pair.key_) {
                        insert_index = (insert_index + 1) & content_mask;
                    } else if (cas128((__uint128_t *) data + insert_index, exist.whole, insert_value.whole)) {
                        insert_value = exist;
                        insert_index = (insert_index + 1) & content_mask;
                    }
                }
                return 1;
            }

            pair<uint32_t, Entry> _findreplacement(uint32_t start_index) {
                auto content_mask = content_len_ - 1;
                auto ref_index = start_index;
                Entry ref_entry;
                do {
                    ++ref_index;
                    ref_entry = content_[ref_index & content_mask];
                } while (ref_entry.pair.key_ != KTYPE::empty &&
                         (knuth_hash(ref_entry.pair.key_) & content_mask) > start_index);
                auto back_index = ref_index - 1;
                while (back_index > start_index) {
                    auto cand_entry = content_[back_index & content_mask];
                    if (cand_entry.pair.key_ == KTYPE::empty ||
                        (knuth_hash(cand_entry.pair.key_) & content_mask) <= start_index) {
                        ref_index = back_index;
                        ref_entry = cand_entry;
                    }
                    --back_index;
                }
                return pair<uint32_t, Entry>(ref_index, ref_entry);
            }

        public:

            PhaseConcurrentHashMap() : PhaseConcurrentHashMap(524288) {}

            PhaseConcurrentHashMap(uint32_t expect_size) : size_(0) {
                content_len_ = ceil2(expect_size * SCALE);
                content_ = (Entry *) aligned_alloc(16, sizeof(Entry) * content_len_);
                memset(content_, -1, sizeof(Entry) * content_len_);
            }

            virtual~ PhaseConcurrentHashMap() {
                for (uint32_t i = 0; i < content_len_; ++i) {
                    if (content_[i].pair.key_ != KTYPE::empty)
                        delete content_[i].pair.value_;
                }
                free(content_);
            }

            PhaseConcurrentHashMap(PhaseConcurrentHashMap &) = delete;

            PhaseConcurrentHashMap(PhaseConcurrentHashMap &&) = delete;

            PhaseConcurrentHashMap &operator=(PhaseConcurrentHashMap &) = delete;

            PhaseConcurrentHashMap &operator=(PhaseConcurrentHashMap &&) = delete;

            void put(ktype key, VTYPEP value) {
                Entry entry;
                entry.pair = {key, value};
                if (_insert(content_, content_len_, entry)) {
                    size_++;
                    if (size_ >= limit()) {
                        // TODO What error?
                        throw std::invalid_argument("hash map is full");
                    }
                }
            }

            VTYPEP get(ktype key) {
                auto content_size = content_len_;
                auto content_mask = content_size - 1;
                auto index = knuth_hash(key) & content_mask;
                while (content_[index].pair.key_ != KTYPE::empty && content_[index].pair.key_ != key) {
                    index = (index + 1) & content_mask;
                }
                if (content_[index].pair.key_ == KTYPE::empty) {
                    return nullptr;
                }
                return content_[index].pair.value_;
            }

            VTYPEP remove(ktype key) {
                auto content_size = content_len_;
                auto content_mask = content_size - 1;
                int base_index = knuth_hash(key) & content_mask;
                int ref_index = base_index;
                Entry ref_entry;
                ref_entry.pair.key_ = key;
                VTYPEP retval = nullptr;
                bool cas_success = false;
                while (content_[ref_index & content_mask].pair.key_ != KTYPE::empty &&
                       key < content_[ref_index & content_mask].pair.key_) {
                    ref_index += 1;
                }
                while (ref_index >= base_index) {
                    if (ref_entry.pair.key_ == KTYPE::empty ||
                        ref_entry.pair.key_ != content_[ref_index & content_mask].pair.key_) {
                        ref_index -= 1;
                    } else {
                        ktype ref_key = ref_entry.pair.key_;
                        ref_entry = content_[ref_index & content_mask];
                        ref_entry.pair.key_ = ref_key;
                        auto cand = _findreplacement(ref_index);
                        auto cand_index = cand.first;
                        auto cand_entry = cand.second;
                        if (cas128((__uint128_t *) (content_ + (ref_index & content_mask)),
                                   ref_entry.whole, cand_entry.whole)) {
                            cas_success = true;
                            if (!retval) {
                                retval = ref_entry.pair.value_;
                            }
                            if (cand_entry.pair.key_ != KTYPE::empty) {
                                base_index = knuth_hash(cand_entry.pair.key_) & content_mask;
                                ref_index = cand_index;
                                ref_entry = cand_entry;
                            } else {
                                if (cas_success) {
                                    size_--;
                                }
                                return retval;
                            }
                        } else {
                            ref_index -= 1;
                        }
                    }
                }
                if (cas_success) {
                    size_--;
                }
                return retval;
            }

            void resize(uint64_t expect) {
                auto new_size = ceil2(expect);

                auto new_content_len = ceil2(new_size);
                auto new_content = (Entry *) aligned_alloc(16, sizeof(Entry) * new_content_len);
                memset(content_, -1, sizeof(Entry) * new_content_len);

                // Rehash all elements
                for (uint32_t i = 0; i < content_len_; ++i) {
                    auto entry = content_[i];
                    if (entry.key_ != KTYPE::empty) {
                        _insert(new_content, new_content_len, entry);
                    }
                }
                free(content_);
                content_ = new_content;
                content_len_ = new_content_len;
            }

            class PCHMIterator : public Iterator<pair<ktype, VTYPEP>> {
                Entry *content_;
                uint32_t content_len_;
                uint32_t pointer_;
            public:
                PCHMIterator(Entry *content, uint32_t content_len) : content_(content), content_len_(content_len),
                                                                     pointer_(0) {
                    while (pointer_ < content_len_ && content_[pointer_].pair.key_ == KTYPE::empty) {
                        ++pointer_;
                    }
                }

                virtual ~PCHMIterator() = default;

                bool hasNext() {
                    return pointer_ < content_len_;
                }

                pair<ktype, VTYPEP> next() {
                    Entry value = content_[pointer_];
                    do { ++pointer_; } while (pointer_ < content_len_ && content_[pointer_].pair.key_ == KTYPE::empty);
                    return pair<ktype, VTYPEP>{value.pair.key_, value.pair.value_};
                }
            };

            unique_ptr<Iterator<pair<ktype, VTYPEP>>> iterator() {
                return unique_ptr<Iterator<pair<ktype, VTYPEP>>>(new PCHMIterator(content_, content_len_));
            }

            inline uint32_t size() { return size_; }

            inline uint32_t limit() { return content_len_; }
        };
    }
}
#endif //ARROW_CONTAINER_H
