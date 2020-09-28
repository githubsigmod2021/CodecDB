//
// Created by harper on 2/13/20.
//

#ifndef LQF_STREAM_H
#define LQF_STREAM_H

#include <vector>
#include <memory>
#include <iostream>
#include <functional>
#include <optional>
#include <tuple>
#include "threadpool.h"
#include <arrow/util/thread_pool.h>

using namespace std;

///
///
///
namespace lqf {

    using namespace threadpool;
    ///
    /// This is the first attempt to create a stream. It is slow though
    ///
    namespace oldstream {

        ///
        /// We create these operators for parallel execution of stream tasks.
        /// The operator performs lazy evaluation of stream mapper/filters when its eval() function is called.
        /// It is known that such wrapping (involving creating new object/calling virtual function for each
        /// element in the stream) has overhead. The result of stream_benchmark shows the overhead the wrapper
        /// brings is 35ns per unit compared to executing the mappers/filters directly on the element.
        /// \tparam T
        template<typename T>
        class EvalOp {
        public:
            virtual ~EvalOp() = default;

            virtual T eval() = 0;
        };

        template<typename T>
        class FilterOp {
        protected:
            unique_ptr<EvalOp<T>> previous_;
            function<bool(const T &)> filter_;
        public:
            FilterOp(unique_ptr<EvalOp<T>> prev, function<bool(const T &)> f)
                    : previous_(prev), filter_(f) {}

            virtual ~FilterOp() = default;

            inline T eval() override {
                T result = previous_->eval();
                return filter_(result) ? result : nullptr;
            }
        };

        template<typename FROM, typename TO>
        class TransformOp : public EvalOp<TO> {
        public:
            TransformOp(unique_ptr<EvalOp<FROM>> from, function<TO(const FROM &)> f)
                    : previous_(move(from)), mapper_(f) {}

            virtual ~TransformOp() = default;

            inline TO eval() override {
                FROM from = previous_->eval();
                return mapper_(from);
            }

        protected:
            unique_ptr<EvalOp<FROM>> previous_;
            function<TO(const FROM &)> mapper_;
        };

        template<typename TYPE>
        class TrivialOp : public EvalOp<TYPE> {
        private:
            TYPE ref_;
        public:
            TrivialOp(const TYPE ref) : ref_(ref) {}

            virtual ~TrivialOp() = default;

            inline TYPE eval() override {
                return ref_;
            }
        };

        template<typename VIEW>
        class Stream;

        template<typename FROM, typename TO>
        class MapStream;

        template<typename TYPE>
        class FilterStream;

        using namespace lqf::threadpool;

        class StreamEvaluator {
        public:
            static shared_ptr<Executor> defaultExecutor;

            bool parallel_;

            StreamEvaluator() : parallel_(false) {}

            virtual ~StreamEvaluator() = default;

            template<typename T>
            shared_ptr<vector<T>> eval(vector<unique_ptr<EvalOp<T>>> &input) {
                if (parallel_) {
                    return evalParallel(input);
                } else {
                    return evalSequential(input);
                }
            }

            template<typename T>
            static T evalOp(EvalOp<T> *op) {
                return op->eval();
            }

            template<typename T>
            inline shared_ptr<vector<T>> evalParallel(vector<unique_ptr<EvalOp<T>>> &input) {
                vector<function<T()>> tasks;
                for (auto &eval:input) {
                    tasks.push_back(std::bind(&StreamEvaluator::evalOp<T>, eval.get()));
                }
                return defaultExecutor->invokeAll(tasks);
            }

            template<typename T>
            inline shared_ptr<vector<T>> evalSequential(vector<unique_ptr<EvalOp<T>>> &input) {
                auto result = make_shared<vector<T>>();
                for (auto &eval:input) {
                    result->push_back(evalOp(eval.get()));
                }
                return result;
            }
        };

        template<typename VIEW>
        class Stream : public enable_shared_from_this<Stream<VIEW>> {
        public:
            Stream() : evaluator_(nullptr) {}

            virtual ~Stream() = default;

            template<typename NEXT>
            shared_ptr<Stream<NEXT>> map(function<NEXT(const VIEW &)> f) {
                return make_shared<MapStream<VIEW, NEXT>>(this->shared_from_this(), f, evaluator_);
            }

            shared_ptr<Stream<VIEW>> filter(function<bool(const VIEW &)> f) {
                return make_shared<FilterStream<VIEW>>(this->shared_from_this(), f, evaluator_);
            }

            /// Use int32_t as workaround as void cannot be allocated return space
            void foreach(function<void(const VIEW &)> f) {
                if (evaluator_->parallel_) {
                    vector<unique_ptr<EvalOp<int32_t>>> holder;
                    while (!_isEmpty()) {
                        holder.push_back(unique_ptr<EvalOp<int32_t>>(
                                new TransformOp<VIEW, int32_t>(
                                        _next(),
                                        [=](const VIEW &a) {
                                            f(a);
                                            return 0;
                                        })));
                    }
                    evaluator_->eval(holder);
                } else {
                    // Using EvalOp has overhead
                    while (!_isEmpty()) {
                        f(_next2());
                    }
                }
            }

            shared_ptr<vector<VIEW>> collect() {
                if (evaluator_->parallel_) {
                    vector<unique_ptr<EvalOp<VIEW>>> holder;
                    while (!_isEmpty()) {
                        holder.push_back(_next());
                    }
                    auto result = evaluator_->eval(holder);
                    return result;
                } else {
                    shared_ptr<vector<VIEW>> result = make_shared<vector<VIEW>>();
                    while (!_isEmpty()) {
                        result->push_back(_next2());
                    }
                    return result;
                }
            }

            VIEW reduce(function<VIEW(VIEW &, VIEW &)> reducer) {
                auto collected = collect();
                if (collected->size() == 1) {
                    return (*collected)[0];
                }
                // execute reduce in parallel
                while (collected->size() > 1) {
                    vector<unique_ptr<EvalOp<VIEW>>> holder;


                    collected = evaluator_->eval(holder);
                }
                return (*collected)[0];
//                VIEW first = move((*collected)[0]);
//                auto ite = collected->begin();
//                ite++;
//                for (; ite != collected->end(); ite++) {
//                    first = reducer(first, *ite);
//                }
//                return first;
            }

            inline bool isParallel() {
                return evaluator_->parallel_;
            }

            inline shared_ptr<Stream<VIEW>> parallel() {
                evaluator_->parallel_ = true;
                return this->shared_from_this();
            }

            inline shared_ptr<Stream<VIEW>> sequential() {
                evaluator_->parallel_ = false;
                return this->shared_from_this();
            }

            virtual bool _isEmpty() = 0;

            virtual unique_ptr<EvalOp<VIEW>> _next() = 0;

            virtual VIEW _next2() = 0;

            shared_ptr<StreamEvaluator> evaluator_;
        };

        template<typename FROM, typename TO>
        class MapStream : public Stream<TO> {
        public:
            MapStream(shared_ptr<Stream<FROM>> source, function<TO(const FROM &)> mapper,
                      shared_ptr<StreamEvaluator> eval)
                    : source_(source), mapper_(mapper) {
                this->evaluator_ = eval;
            }

            virtual ~MapStream() = default;

            bool _isEmpty() override {
                return source_->_isEmpty();
            }

            unique_ptr<EvalOp<TO>> _next() override {
                return unique_ptr<TransformOp<FROM, TO>>(new TransformOp<FROM, TO>(source_->_next(), mapper_));
            }

            TO _next2() override {
                return mapper_(source_->_next2());
            }

        protected:
            shared_ptr<Stream<FROM>> source_;
            function<TO(const FROM &)> mapper_;
        };

        template<typename TYPE>
        class FilterStream : public Stream<TYPE> {
        public:
            FilterStream(shared_ptr<Stream<TYPE>> source, function<bool(const TYPE &)> filter,
                         shared_ptr<StreamEvaluator> eval)
                    : source_(source), filter_(filter) {
                this->evaluator_ = eval;
            }

            virtual ~FilterStream() = default;

            bool _isEmpty() override {
                return source_->isEmpty();
            }

            unique_ptr<EvalOp<TYPE>> _next() override {
                return unique_ptr<FilterOp<TYPE>>(new FilterOp<TYPE>(source_->next(), filter_));
            }

            TYPE _next2() override {
                auto next = source_->next2();
                if (filter_(next)) {
                    return next;
                }
                return nullptr;
            }

            shared_ptr<Stream<TYPE>> source_;
            function<bool(const TYPE &)> filter_;
        };

        template<typename TYPE>
        class VectorStream : public Stream<TYPE> {
        private:
            const vector<TYPE> &data_;
            typename vector<TYPE>::const_iterator position_;
        public:
            VectorStream(const vector<TYPE> &data) :
                    data_(data), position_(data.begin()) {
                this->evaluator_ = make_shared<StreamEvaluator>();
            }

            virtual ~VectorStream() = default;

            bool _isEmpty() override {
                return position_ == data_.end();
            }

            unique_ptr<EvalOp<TYPE>> _next() override {
                return unique_ptr<EvalOp<TYPE>>(new TrivialOp<TYPE>(*(position_++)));
            };

            TYPE _next2() override {
                return *(position_++);
            }
        };

        class IntStream : public Stream<int32_t> {

        public:
            static shared_ptr<IntStream> Make(int32_t from, int32_t to) {
                return make_shared<IntStream>(from, to, 1);
            }

            IntStream(int32_t from, int32_t to, int32_t step = 1)
                    : to_(to), step_(step), pointer_(from) {
                this->evaluator_ = make_shared<StreamEvaluator>();
            }

            virtual ~IntStream() = default;

            bool _isEmpty() override {
                return pointer_ >= to_;
            }

            unique_ptr<EvalOp<int32_t>> _next() override {
                int32_t value = pointer_;
                pointer_ += step_;
                return unique_ptr<TrivialOp<int32_t>>(new TrivialOp<int32_t>(value));
            }

            int32_t _next2() override {
                int32_t value = pointer_;
                pointer_ += step_;
                return value;
            }

        private:
            int32_t to_;
            int32_t step_;
            int32_t pointer_;
        };
    }

    namespace mapper {
        template<typename DST, typename SRC>
        class Mapper {
        public:
            virtual DST operator()(SRC in) = 0;

            virtual ~Mapper() = default;
        };

        template<typename DST, typename SRC>
        class Cast : public Mapper<DST, SRC> {
        public:
            Cast() {}

            virtual ~Cast() = default;

            DST operator()(SRC in) override {
                return static_cast<DST>(in);
            }
        };

        template<typename DST, typename SRC>
        class PointerCast : public Mapper<DST, SRC> {
        public:
            PointerCast() {}

            virtual ~PointerCast() = default;

            DST operator()(SRC in) override {
                return reinterpret_cast<DST>(in);
            }
        };

        template<typename T>
        class VectorGet : public Mapper<T, uint64_t> {
        protected:
            vector<T> &content_;
        public:
            VectorGet(vector<T> &content) : content_(content) {}

            virtual ~VectorGet() = default;

            T operator()(uint64_t in) override {
                return content_[static_cast<int32_t>(in)];
            }
        };

        template<typename OUT, typename IN, typename SRC>
        class TransformMapper : public Mapper<OUT, SRC> {
        protected:
            function<OUT(const IN &)> map_;
            unique_ptr<Mapper<IN, SRC>> inner_;
        public:
            TransformMapper(unique_ptr<Mapper<IN, SRC>> inner, function<OUT(const IN &)> map)
                    : map_(map), inner_(move(inner)) {}

            virtual ~TransformMapper() = default;

            OUT operator()(SRC in) override {
                return map_((*inner_)(in));
            }
        };

        template<typename IN, typename SRC>
        class EvalMapper : public Mapper<void, SRC> {
        protected:
            function<void(const IN &)> map_;
            unique_ptr<Mapper<IN, SRC>> inner_;
        public:
            EvalMapper(unique_ptr<Mapper<IN, SRC>> inner, function<void(const IN &)> map)
                    : map_(map), inner_(move(inner)) {}

            virtual ~EvalMapper() = default;

            void operator()(SRC in) override {
                map_((*inner_)(in));
            }
        };
    }

    using namespace mapper;

    template<typename T>
    class StreamSource {
    public:
        virtual ~StreamSource() = default;

        virtual bool hasNext() = 0;

        virtual T next() = 0;
    };

    class StreamEvaluator {
    public:
        static shared_ptr<Executor> defaultExecutor;
//        static shared_ptr<arrow::internal::ThreadPool> defaultExecutor;

        bool parallel_;

        StreamEvaluator() : parallel_(false) {}

        virtual ~StreamEvaluator() = default;

        template<typename T, typename SRC>
        unique_ptr<vector<T>> collect(StreamSource<SRC> *source, Mapper<T, SRC> *mapper) {
            if (parallel_) {
                // Version 1: LQF threadpool
                vector<function<T()>> tasks;
                while (source->hasNext()) {
                    auto next = source->next();
                    tasks.push_back([=]() {
                        return (*mapper)(next);
                    });
                }
                return move(defaultExecutor->invokeAll(tasks));
                // Version 2: serial
//                auto result = unique_ptr<vector<T>>(new vector<T>());
//                while (source->hasNext()) {
//                    result->push_back((*mapper)(source->next()));
//                }
//                return result;
                // Version 3: std::async
//                vector<future<T>> futures;
//                while (source->hasNext()) {
//                    auto next = source->next();
//                    futures.push_back(std::async(std::launch::async, [=](SRC n){return (*mapper)(n);}, next));
//                }
//                auto results = new vector<T>();
//                for (auto &future:futures) {
//                    results->push_back(future.get());
//                }
//                return unique_ptr<vector<T>>(results);
                // Version 4: Arrow Threadpool
//                vector<future<T>> futures;
//                while (source->hasNext()) {
//                    futures.push_back(*(defaultExecutor->Submit([=](uint32_t next) { return (*mapper)(next); } ,source->next())));
//                }
//                auto results = new vector<T>();
//                for (auto &future:futures) {
//                    results->push_back(future.get());
//                }
//                return unique_ptr<vector<T>>(results);
            } else {
                auto result = unique_ptr<vector<T>>(new vector<T>());
                while (source->hasNext()) {
                    result->push_back((*mapper)(source->next()));
                }
                return result;
            }
        }

        template<typename SRC>
        void eval(StreamSource<SRC> *source, Mapper<void, SRC> *mapper) {
            if (parallel_) {
                // Lqf Threadpool
                vector<function<int()>> tasks;
                while (source->hasNext()) {
                    auto next = source->next();
                    tasks.push_back([=]() {
                        (*mapper)(next);
                        return 0;
                    });
                }
                defaultExecutor->invokeAll(tasks);
                // Serial
//                while (source->hasNext()) {
//                    (*mapper)(source->next());
//                }
//                vector<future<void>> futures;
//                while (source->hasNext()) {
//                    auto next = source->next();
//                    futures.push_back(std::async(std::launch::async, [=](SRC n){(*mapper)(n);}, next));
//                }
//                for (auto &future:futures) {
//                    future.wait();
//                }
                // Arrow Threadpool
//                vector<future<void>> futures;
//                while (source->hasNext()) {
//                    futures.push_back(*(defaultExecutor->Submit([=](uint32_t next) { (*mapper)(next); } ,source->next())));
//                }
//                for (auto &future:futures) {
//                    future.wait();
//                }
            } else {
                while (source->hasNext()) {
                    (*mapper)(source->next());
                }
            }
        }

        template<typename T, typename SRC, typename REDUCER>
        T reduce(StreamSource<SRC> *source, Mapper<T, SRC> *mapper, REDUCER reducer) {
            if (parallel_) {

                // execute reduce in parallel
                auto collected = collect(source, mapper);
                while (collected->size() > 1) {
                    vector<function<T()>> tasks;
                    auto pair = collected->size() / 2;
                    auto remain = collected->size() % 2;
                    for (auto i = 0u; i < pair; ++i) {
                        auto first = move((*collected)[i * 2]);
                        auto second = move((*collected)[i * 2 + 1]);
                        tasks.push_back([&reducer, first, second]() {
                            return reducer(first, second);
                        });
                    }
                    auto next = defaultExecutor->invokeAll(tasks);
                    if (remain) {
                        next->emplace_back(move(collected->back()));
                    }
                    collected = move(next);
                }
                return move((*collected)[0]);
            } else {
                T result = (*mapper)(source->next());
                while (source->hasNext()) {
                    auto next = (*mapper)(source->next());
                    result = reducer(result, next);
                }
                return result;
            }
        }
    };

    template<typename T, typename SRC>
    class StreamBase {
    protected:
        unique_ptr<Mapper<T, SRC>> mapper_;
        unique_ptr<StreamSource<SRC>> source_;
        unique_ptr<StreamEvaluator> evaluator_;
    public:
        StreamBase(Mapper<T, SRC> *mapper, unique_ptr<StreamSource<SRC>> source, unique_ptr<StreamEvaluator> eval)
                : mapper_(unique_ptr<Mapper<T, SRC>>(mapper)), source_(move(source)), evaluator_(move(eval)) {}

        virtual ~StreamBase() = default;

        template<typename TO>
        unique_ptr<StreamBase<TO, SRC>> map(function<TO(const T &)> mapmore) {
            return unique_ptr<StreamBase<TO, SRC>>(
                    new StreamBase<TO, SRC>(new TransformMapper<TO, T, SRC>(move(mapper_), mapmore), move(source_),
                                            move(evaluator_)));
        }

        void foreach(function<void(const T &)> f) {
            auto mapper = unique_ptr<Mapper<void, SRC>>(new EvalMapper<T, SRC>(move(mapper_), f));
            evaluator_->eval(source_.get(), mapper.get());
        }

        unique_ptr<vector<T>> collect() {
            return evaluator_->collect(source_.get(), mapper_.get());
        }

        // For simplicity we assume there is at least one valid element
        template<typename REDUCER>
        T reduce(REDUCER reducer) {
            return evaluator_->reduce(source_.get(), mapper_.get(), reducer);
        }

        inline bool isParallel() {
            return evaluator_->parallel_;
        }

        unique_ptr<StreamBase<T, SRC>> parallel() {
            evaluator_->parallel_ = true;
            return unique_ptr<StreamBase<T, SRC>>(
                    new StreamBase<T, SRC>(mapper_.release(), move(source_), move(evaluator_)));
        }

        unique_ptr<StreamBase<T, SRC>> sequential() {
            evaluator_->parallel_ = false;
            return unique_ptr<StreamBase<T, SRC>>(
                    new StreamBase<T, SRC>(mapper_.release(), move(source_), move(evaluator_)));
        }
    };

    template<typename T>
    using Stream = StreamBase<T, uint64_t>;

    class IntSource : public StreamSource<uint64_t> {
    protected:
        int32_t pointer_;
        int32_t to_;
        int32_t step_;
    public:
        IntSource(int32_t from, int32_t to, int32_t step = 1) : pointer_(from), to_(to), step_(step) {}

        virtual ~IntSource() = default;

        bool hasNext() override {
            return pointer_ < to_;
        }

        uint64_t next() override {
            int result = pointer_;
            pointer_ += step_;
            return result;
        }
    };

    class IntStream : public Stream<int32_t> {
    public:
        IntStream(int32_t from, int32_t to, int32_t step = 1)
                : StreamBase(new Cast<int32_t, uint64_t>(), unique_ptr<IntSource>(new IntSource(from, to, step)),
                             unique_ptr<StreamEvaluator>(new StreamEvaluator())) {}

        virtual ~IntStream() = default;

        static unique_ptr<IntStream> Make(int from, int to) {
            return unique_ptr<IntStream>(new IntStream(from, to));
        }
    };

    template<typename T>
    class VectorStream : public Stream<T> {
    public:
        VectorStream(vector<T> &source) :
                Stream<T>(new VectorGet<T>(source), unique_ptr<IntSource>(new IntSource(0, source.size())),
                          unique_ptr<StreamEvaluator>(new StreamEvaluator())) {}

        virtual ~VectorStream() = default;
    };

    namespace typeless {
        // Experimental Development.
        // This requires C++ 17 to work

        // Update: this does not help as we need a single return type of Stream for inheritances in data model.
        // In the main design, the type of a stream is determined by its source and its output type.
        // In this design, the type of a stream is determined by the source and its mapper.
        // We always have the same output type, so limiting the source to a unique type (e.g., uint64_t) can make the Stream type unique.
        // But in this design, the mappers are all different and there's no way to make the streams all follow the same interface.
        template<typename PREV, typename MAPPER>
        class MapNode {
        protected:
            PREV previous_;
            MAPPER mapper_;
        public:
            MapNode(PREV prev, MAPPER mapf)
                    : previous_(move(prev)), mapper_(mapf) {}

            virtual ~MapNode() = default;

            template<typename IN>
            auto operator()(IN input) {
                return mapper_(previous_(input));
            }
        };

        template<typename MAPPER>
        class MapHead {
        protected:
            MAPPER mapper_;
        public:
            MapHead(MAPPER mapf) : mapper_(mapf) {}

            virtual ~MapHead() = default;

            template<typename IN>
            auto operator()(IN input) {
                return mapper_(input);
            }
        };

        template<typename MAPPER, typename SRC>
        class Stream {
        protected:
            MAPPER mapper_;
            SRC source_;
        public:
            virtual ~Stream() = default;

            template<typename MAP>
            unique_ptr<Stream> map(MAP mapper) {
                return nullptr;
            }

            template<typename EVAL>
            void foreach(EVAL eval) {

            }

            template<typename T, typename REDUCER>
            T reduce(REDUCER reducer) {
                return nullptr;
            }

            template<typename T>
            shared_ptr<vector<T>> collect() {

            }
        };
    }
}
#endif //CHIDATA_STREAM_H
