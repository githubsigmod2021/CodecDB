//
// Created by Harper on 4/29/20.
//

#ifndef ARROW_CONCURRENT_H
#define ARROW_CONCURRENT_H

#include <cstdint>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>
#include <thread>
#include <unordered_map>
#include <memory>
#include <functional>

namespace lqf {
    namespace concurrent {
        class Semaphore {
        private:
            std::mutex mutex_;
            std::condition_variable condition_;
            uint32_t count_ = 0; // Initialized as locked.

        public:
            virtual ~Semaphore() = default;

            void notify();

            void notify(uint32_t num);

            void wait();

            void wait(uint32_t num);

            bool try_wait();

            bool try_wait(uint32_t num);
        };

        template<typename T>
        class ThreadLocal {
        protected:
            std::function<T()> init_;
            std::unordered_map<std::thread::id, std::shared_ptr<T>> content_;
            std::shared_mutex access_lock_;
        public:
            ThreadLocal(std::function<T()> init) : init_(init) {}

            std::shared_ptr<T> get() {
                auto this_thread_id = std::this_thread::get_id();
                access_lock_.lock_shared();
                auto found = content_.find(this_thread_id);
                if (found != content_.end()) {
                    access_lock_.unlock_shared();
                    return found->second;
                } else {
                    access_lock_.unlock_shared();
                    access_lock_.lock();
                    auto created = std::make_shared<T>(init_());
                    content_[this_thread_id] = created;
                    access_lock_.unlock();
                    return created;
                }
            }
        };
    }
}


#endif //ARROW_CONCURRENT_H
