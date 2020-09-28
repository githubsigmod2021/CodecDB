//
// Created by harper on 3/14/20.
//

#include "stream.h"

namespace lqf {

    shared_ptr<Executor> StreamEvaluator::defaultExecutor = Executor::Make(20);

//    using namespace arrow::internal;
//    shared_ptr<ThreadPool> StreamEvaluator::defaultExecutor = *(ThreadPool::Make(25));
}