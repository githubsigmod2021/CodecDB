//
// Created by harper on 3/14/20.
//

#ifndef ARROW_THREADPOOL_H
#define ARROW_THREADPOOL_H

#include<thread>
#include<memory>
#include<queue>
#include<vector>
#include<mutex>
#include<future>
#include<functional>
#include <condition_variable>
#include "concurrent.h"

using namespace std;

namespace lqf {
    using namespace concurrent;
    namespace threadpool {

        class Executor;

        class Task {
            friend Executor;
        protected:
            function<void()> runnable_;
        public:
            Task(function<void()> runnable) : runnable_(runnable) {}

            virtual ~Task() = default;

            inline void run() {
                runnable_();
            }
        };

        template<typename T>
        class CallTask : public Task {
            friend Executor;
        private:
            promise<T> promise_;
        public:
            CallTask(function<T()> callable)
                    : Task(bind(&CallTask::call, this)), callable_(callable) {}

            virtual ~CallTask() = default;

        protected:
            function<T()> callable_;

            void call() {
                promise_.set_value(callable_());
            }

            inline future<T> getFuture() {
                return promise_.get_future();
            }
        };

        class Executor {
        protected:
            atomic<bool> shutdown_;
            Semaphore shutdown_guard_;
            uint32_t pool_size_;
            vector<unique_ptr<thread>> threads_;
            Semaphore has_task_;
            mutex fetch_task_;
            queue<unique_ptr<Task>> tasks_;

            void submit(unique_ptr<Task>);

        public:
            inline uint32_t pool_size() { return pool_size_; }

            virtual ~Executor();

            void shutdown();

            void submit(function<void()>);

            template<typename T>
            future<T> submit(function<T()> task) {
                auto t = new CallTask<T>(task);
                submit(unique_ptr<CallTask<T>>(t));
                return t->getFuture();
            }

            template<typename T>
            unique_ptr<vector<T>> invokeAll(vector<function<T()>> &tasks) {
                vector<future<T>> futures;
                for (auto& t: tasks) {
                    auto res = new CallTask<T>(t);
                    futures.push_back(res->getFuture());
                    submit(unique_ptr<CallTask<T>>(res));
                }
                unique_ptr<vector<T>> result = unique_ptr<vector<T>>(new vector<T>());
                for (auto &future:futures) {
                    future.wait();
                    result->push_back(future.get());
                }
                return result;
            }

            static shared_ptr<Executor> Make(uint32_t psize);

            Executor(uint32_t pool_size);

        protected:
            void routine();
        };
    }
}
#endif //ARROW_THREADPOOL_H
