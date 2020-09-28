//
// Created by harper on 3/14/20.
//

#include <iostream>
#include "threadpool.h"

namespace lqf {
    namespace threadpool {

        Executor::Executor(uint32_t pool_size) : shutdown_(false), pool_size_(pool_size), threads_() {
            for (uint32_t i = 0; i < pool_size; ++i) {
                auto t = new std::thread(bind(&Executor::routine, this));
                t->detach();
                threads_.push_back(unique_ptr<std::thread>(t));
            }
        }

        Executor::~Executor() {
            if(!shutdown_) {
                shutdown();
            }
            threads_.clear();
        }

        void Executor::shutdown() {
            shutdown_ = true;
            /// Wake up threads waiting for tasks
            for (uint32_t i = 0; i < pool_size_; ++i) {
                has_task_.notify();
            }
            shutdown_guard_.wait(pool_size_);
        }

        shared_ptr<Executor> Executor::Make(uint32_t psize) {
            return make_shared<Executor>(psize);
        }

        void Executor::submit(unique_ptr<Task> task) {
            if(shutdown_) {
                return;
            }
            fetch_task_.lock();
            tasks_.push(move(task));
            has_task_.notify();
            fetch_task_.unlock();
        }

        void Executor::submit(function<void()> runnable) {
            submit(unique_ptr<Task>(new Task(runnable)));
        }

        void Executor::routine() {
            while (!shutdown_) {
                has_task_.wait();

                fetch_task_.lock();
                if (tasks_.empty()) {
                    fetch_task_.unlock();
                } else {
                    auto task = move(tasks_.front());
                    tasks_.pop();
                    fetch_task_.unlock();

                    task->run();
                }
            }
            shutdown_guard_.notify();
        }


    }
}