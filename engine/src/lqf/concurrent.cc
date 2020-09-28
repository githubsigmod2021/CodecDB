//
// Created by Harper on 4/29/20.
//

#include "concurrent.h"

namespace lqf {
    namespace concurrent {

        void Semaphore::notify() {
            std::lock_guard<decltype(mutex_)> lock(mutex_);
            ++count_;
            condition_.notify_one();
        }

        void Semaphore::notify(uint32_t num) {
            std::lock_guard<decltype(mutex_)> lock(mutex_);
            count_ += num;
            for (uint32_t i = 0; i < num; ++i) {
                condition_.notify_one();
            }
        }

        void Semaphore::wait() {
            std::unique_lock<decltype(mutex_)> lock(mutex_);
            while (!count_) // Handle spurious wake-ups.
                condition_.wait(lock);
            --count_;
        }

        void Semaphore::wait(uint32_t num) {
            std::unique_lock<decltype(mutex_)> lock(mutex_);
            uint32_t remain = num;
            while (remain) {
                while (!count_) {
                    condition_.wait(lock);
                }
                auto delta = std::min(remain, count_);
                remain -= delta;
                count_ -= delta;
            }
        }

        bool Semaphore::try_wait() {
            std::lock_guard<decltype(mutex_)> lock(mutex_);
            if (count_) {
                --count_;
                return true;
            }
            return false;
        }

        bool Semaphore::try_wait(uint32_t num) {
            std::lock_guard<decltype(mutex_)> lock(mutex_);
            if (count_ > num) {
                count_ -= num;
                return true;
            }
            return false;
        }
    }
}