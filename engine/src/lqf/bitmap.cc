//
// Created by harper on 2/5/20.
//

#include <assert.h>
#include <cstring>
#include <immintrin.h>
#include "validate.h"
#include "bitmap.h"
#include <sboost/simd.h>

namespace lqf {

    Bitset::Bitset(uint64_t value) : value_(value) {}

    uint32_t Bitset::size() {
        return __builtin_popcount(value_);
    }

    uint32_t Bitset::next() {
        uint64_t t = value_ & -value_; // isolate rightmost 1
        uint32_t result = __builtin_popcount(t - 1);
        value_ ^= t;
        return result;
    }

    bool Bitset::hasNext() {
        return value_ != 0;
    }

    SimpleBitmapIterator::SimpleBitmapIterator(uint64_t *content, uint64_t content_size, uint64_t num_bits) {
        this->content_ = content;
        this->content_size_ = content_size;
        this->num_bits_ = num_bits;
        this->final_mask_ = (1L << (num_bits & 0x3f)) - 1;

        for (pointer_ = 0; pointer_ < content_size_; ++pointer_) {
            if ((cached_ = content_[pointer_]) != 0) {
                break;
            }
        }
    }

    void SimpleBitmapIterator::moveTo(uint64_t pos) {
        pointer_ = pos >> 6;
        // Find next non-zero pointer
        if (content_[pointer_] != 0) {
            uint64_t remain = pos & 0x3F;
            cached_ = content_[pointer_];
            cached_ &= 0xFFFFFFFFFFFFFFFFL << remain;
            while (cached_ == 0) {
                ++pointer_;
                if (pointer_ == content_size_) {
                    break;
                }
                cached_ = content_[pointer_];
            }
        } else {
            while (content_[pointer_] == 0 && pointer_ < content_size_) {
                pointer_++;
            }
            if (pointer_ < content_size_) {
                cached_ = content_[pointer_];
            }
        }
    }

    bool SimpleBitmapIterator::hasNext() {
        return pointer_ < content_size_ - 1 ||
               (pointer_ == content_size_ - 1 && (cached_ & final_mask_) != 0);
    }

    uint64_t SimpleBitmapIterator::next() {
        uint64_t t = cached_ & -cached_; // isolate rightmost 1
        uint64_t answer = (pointer_ << 6) + _mm_popcnt_u64(t - 1);
        cached_ ^= t;
        while (cached_ == 0) {
            ++pointer_;
            if (pointer_ == content_size_) {
                break;
            }
            cached_ = content_[pointer_];
        }
        return answer;
    }

    FullBitmapIterator::FullBitmapIterator(uint64_t size) {
        this->size_ = size;
        this->counter_ = 0;
    }

    void FullBitmapIterator::moveTo(uint64_t pos) {
        this->counter_ = pos;
    }

    bool FullBitmapIterator::hasNext() {
        return this->counter_ < size_;
    }

    uint64_t FullBitmapIterator::next() {
        return this->counter_++;
    }

    SimpleBitmap::SimpleBitmap(uint64_t size) {
//        validate_true(size < 0xFFFFFFFFL, "size overflow");
        // Attention: Due to a glitch in sboost, the bitmap should be one word larger than
        // the theoretical size. Otherwise sboost will read past the boundary and cause
        // memory issues.
        // In addition, the RLE encoding may have at most 503 entries appended to the tail.
        // For simplicity, we just make the bitmap large enough.
        array_size_ = (size >> 6) + 10;
//        bitmap_ = (uint64_t *) malloc(sizeof(uint64_t) * array_size_);
        bitmap_ = (uint64_t *) aligned_alloc(64, sizeof(uint64_t) * array_size_);
        memset(bitmap_, 0, sizeof(uint64_t) * array_size_);
        size_ = (int) size;
    }

//    SimpleBitmap::SimpleBitmap(const SimpleBitmap &copy) {
//        array_size_ = copy.array_size_;
//        size_ = copy.size_;
//        first_valid_ = copy.first_valid_;
//        bitmap_ = (uint64_t *) aligned_alloc(64, array_size_);
//        memcpy((void *) bitmap_, (void *) copy.bitmap_, sizeof(uint64_t) * array_size_);
//    }

    SimpleBitmap::SimpleBitmap(SimpleBitmap &&move) {
        array_size_ = move.array_size_;
        size_ = move.size_;
        first_valid_ = move.first_valid_;
        bitmap_ = move.bitmap_;
        move.bitmap_ = nullptr;
    }

    SimpleBitmap::~SimpleBitmap() {
        if (bitmap_ != nullptr)
            free(bitmap_);
        bitmap_ = nullptr;
    }

//    SimpleBitmap &SimpleBitmap::operator=(const SimpleBitmap &copy) {
//        if (bitmap_ != nullptr)
//            free(bitmap_);
//        array_size_ = copy.array_size_;
//        size_ = copy.size_;
//        first_valid_ = copy.first_valid_;
//        bitmap_ = (uint64_t *) aligned_alloc(64, array_size_);
//        memcpy((void *) bitmap_, (void *) copy.bitmap_, sizeof(uint64_t) * array_size_);
//    }

    SimpleBitmap &SimpleBitmap::operator=(SimpleBitmap &&move) {
        if (bitmap_ != nullptr)
            free(bitmap_);
        array_size_ = move.array_size_;
        size_ = move.size_;
        first_valid_ = move.first_valid_;
        bitmap_ = move.bitmap_;
        move.bitmap_ = nullptr;
        return *this;
    }

    bool SimpleBitmap::check(uint64_t pos) {
        uint32_t index = static_cast<uint32_t> (pos >> 6);
        uint32_t offset = static_cast<uint32_t> (pos & 0x3F);
        return (bitmap_[index] & (1L << offset)) != 0;
    }

    void SimpleBitmap::put(uint64_t pos) {
        uint32_t index = static_cast<uint32_t>(pos >> 6);
        uint32_t offset = static_cast<uint32_t> (pos & 0x3F);
        bitmap_[index] |= 1L << offset;
    }

    void SimpleBitmap::clear() {
        memset(bitmap_, 0, sizeof(uint64_t) * array_size_);
    }

    shared_ptr<Bitmap> SimpleBitmap::operator&(Bitmap &another) {
        SimpleBitmap &sx1 = static_cast<SimpleBitmap &>(another);
        this->first_valid_ = -1;
//        uint64_t limit = (array_size_ >> 3) << 3;
//        uint64_t i = 0;
//        for (i = 0; i < limit; i += 8) {
//            __m512i a = _mm512_load_si512((__m512i *) (this->bitmap_ + i));
//            __m512i b = _mm512_load_si512((__m512i *) (sx1.bitmap_ + i));
//            __m512i res = _mm512_and_si512(a, b);
//            _mm512_store_si512((__m512i *) (this->bitmap_ + i), res);
//        }
//        for (; i < array_size_; ++i) {
//            this->bitmap_[i] &= sx1.bitmap_[i];
//        }
        sboost::simd::simd_and(bitmap_, sx1.bitmap_, array_size_);
        return shared_from_this();
    }

    shared_ptr<Bitmap> SimpleBitmap::operator|(Bitmap &another) {
        SimpleBitmap &sx1 = static_cast<SimpleBitmap &>(another);
        this->first_valid_ = -1;
//        uint64_t limit = (array_size_ >> 3) << 3;
//        uint64_t i = 0;
//        for (i = 0; i < limit; i += 8) {
//            __m512i a = _mm512_load_si512((__m512i *) (this->bitmap_ + i));
//            __m512i b = _mm512_load_si512((__m512i *) (sx1.bitmap_ + i));
//            __m512i res = _mm512_or_si512(a, b);
//            _mm512_store_si512((__m512i *) (this->bitmap_ + i), res);
//        }
//        for (; i < array_size_; ++i) {
//            this->bitmap_[i] |= sx1.bitmap_[i];
//        }
        sboost::simd::simd_or(bitmap_, sx1.bitmap_, array_size_);
        return shared_from_this();
    }

    shared_ptr<Bitmap> SimpleBitmap::operator^(Bitmap &another) {
        SimpleBitmap &sx1 = static_cast<SimpleBitmap &>(another);
//        validate_true(size_ == sx1.size_, "size not the same");
        this->first_valid_ = -1;
        uint64_t limit = (array_size_ >> 3) << 3;
        uint64_t i = 0;
        for (i = 0; i < limit; i += 8) {
            __m512i a = _mm512_load_si512((__m512i *) (this->bitmap_ + i));
            __m512i b = _mm512_load_si512((__m512i *) (sx1.bitmap_ + i));
            __m512i res = _mm512_xor_si512(a, b);
            _mm512_store_si512((__m512i *) (this->bitmap_ + i), res);
        }
        for (; i < array_size_; ++i) {
            this->bitmap_[i] ^= sx1.bitmap_[i];
        }
        return shared_from_this();
    }

    shared_ptr<Bitmap> SimpleBitmap::operator~() {
        this->first_valid_ = -1;
        uint64_t limit = (array_size_ >> 3) << 3;
        uint64_t i = 0;
        __m512i ONE = _mm512_set1_epi64(-1);
        for (i = 0; i < limit; i += 8) {
            __m512i a = _mm512_load_si512((__m512i *) (this->bitmap_ + i));
            __m512i res = _mm512_xor_si512(a, ONE);
            _mm512_store_si512((__m512i *) (this->bitmap_ + i), res);
        }
        for (; i < array_size_; ++i) {
            this->bitmap_[i] ^= -1;
        }
        return shared_from_this();
    }

    uint64_t SimpleBitmap::cardinality() {
        uint64_t counter = 0;
        for (uint64_t i = 0; i < array_size_; i++) {
            counter += _mm_popcnt_u64(bitmap_[i]);
        }
        return counter;
    }

    uint64_t SimpleBitmap::size() {
        return size_;
    }

    bool SimpleBitmap::isFull() {
        return size_ == cardinality();
    }

    bool SimpleBitmap::isEmpty() {
        return cardinality() == 0;
    }

    double SimpleBitmap::ratio() {
        return (double) cardinality() / size_;
    }

    std::unique_ptr<BitmapIterator> SimpleBitmap::iterator() {
        return std::unique_ptr<BitmapIterator>(
                new SimpleBitmapIterator(this->bitmap_, this->array_size_, this->size_));
    }

    uint64_t *SimpleBitmap::raw() {
        return bitmap_;
    }

    ConcurrentBitmap::ConcurrentBitmap(uint64_t size) : SimpleBitmap(size) {}

    void ConcurrentBitmap::put(uint64_t pos) {
        uint32_t index = static_cast<uint32_t>(pos >> 6);
        uint32_t offset = static_cast<uint32_t> (pos & 0x3F);
        uint64_t modify = 1L << offset;
        assert(index < array_size_);
        uint64_t oldval = bitmap_[index];
        uint64_t newval;
        do {
            newval = oldval | modify;
        } while (!__atomic_compare_exchange_n(bitmap_ + index, &oldval, newval, false,
                                              std::memory_order_seq_cst, std::memory_order_seq_cst));
    }

    FullBitmap::FullBitmap(uint64_t size) {
        this->size_ = size;
    }

    bool FullBitmap::check(uint64_t pos) {
        return true;
    }

    void FullBitmap::put(uint64_t pos) {
        throw std::invalid_argument("");
    }

    void FullBitmap::clear() {
        throw std::invalid_argument("");
    }

    shared_ptr<Bitmap> FullBitmap::operator&(Bitmap &x1) {
        return x1.shared_from_this();
    }

    shared_ptr<Bitmap> FullBitmap::operator|(Bitmap &x1) {
        return shared_from_this();
    }

    shared_ptr<Bitmap> FullBitmap::operator^(Bitmap &x1) {
        return ~x1;
    }

    shared_ptr<Bitmap> FullBitmap::operator~() {
        return make_shared<SimpleBitmap>(size_);
    }

    uint64_t FullBitmap::cardinality() {
        return size_;
    }

    uint64_t FullBitmap::size() {
        return size_;
    }

    bool FullBitmap::isFull() {
        return true;
    }

    bool FullBitmap::isEmpty() {
        return false;
    }

    double FullBitmap::ratio() {
        return 1;
    }

    std::unique_ptr<BitmapIterator> FullBitmap::iterator() {
        return std::unique_ptr<BitmapIterator>(new FullBitmapIterator(this->size_));
    }

}
