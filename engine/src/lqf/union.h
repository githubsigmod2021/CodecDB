//
// Created by harper on 4/8/20.
//

#ifndef ARROW_UNION_H
#define ARROW_UNION_H

#include "data_model.h"
#include "parallel.h"

namespace lqf {
    using namespace parallel;

    class FilterUnion : public Node {

    public:
        FilterUnion(uint32_t);

        virtual ~FilterUnion() = default;

        unique_ptr<NodeOutput> execute(const vector<NodeOutput *> &) override;

        shared_ptr<Table> execute(const vector<Table *> &);
    };

    class FilterAnd : public Node {
    public:
        FilterAnd(uint32_t);

        virtual ~FilterAnd() = default;

        unique_ptr<NodeOutput> execute(const vector<NodeOutput *> &) override;

        shared_ptr<Table> execute(const vector<Table *> &);
    };
}
#endif //ARROW_UNION_H
