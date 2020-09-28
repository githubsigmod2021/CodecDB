//
// Created by Harper on 5/2/20.
//

#include <gtest/gtest.h>
#include "parallel.h"

using namespace lqf::parallel;
using namespace lqf::threadpool;

using IntOutput = TypedOutput<int32_t>;

class DemoSource : public Node {
protected:
    int32_t value_;
public:
    DemoSource(int32_t value) : Node(0), value_(value) {}

    unique_ptr<NodeOutput> execute(const vector<NodeOutput *> &inputs) override {
        return unique_ptr<NodeOutput>(new IntOutput(value_));
    }
};

class DemoNode : public Node {
public:
    DemoNode(uint32_t num_input) : Node(num_input) {}

    unique_ptr<NodeOutput> execute(const vector<NodeOutput *> &inputs) override {
        int32_t sum = 0;
        for (auto &input: inputs) {
            const auto intval = static_cast<IntOutput *>(input);
            sum += intval->get();
        }
        return unique_ptr<NodeOutput>(new IntOutput(sum));
    }
};

class ExecutionGraphForTest : public ExecutionGraph {
public:
    ExecutionGraphForTest() : ExecutionGraph() {}

    vector<uint32_t> &edgeWeights() {
        return edge_weight_;
    }

    vector<uint8_t> &destination() {
        return destinations_;
    }

    uint32_t num_dest() {
        return num_dest_;
    }

    Executor* getExecutor() {
        return executor_.get();
    }

    void init() {
        ExecutionGraph::init();
    }
};

TEST(ExecutionGraphTest, EdgeWeight) {

    ExecutionGraphForTest graph;

    auto n0 = graph.add(new DemoSource(4), {});
    auto n1 = graph.add(new DemoSource(5), {});
    auto n2 = graph.add(new DemoSource(7), {});
    auto n3 = graph.add(new DemoSource(8), {});

    auto n4 = graph.add(new DemoNode(1), {n0});
    auto n5 = graph.add(new DemoNode(1), {n0});

    auto n6 = graph.add(new DemoNode(2), {n1, n2});

    auto n7 = graph.add(new DemoNode(1), {n3});
    auto n8 = graph.add(new DemoNode(1), {n3});
    auto n9 = graph.add(new DemoNode(1), {n3});

    auto n10 = graph.add(new DemoNode(1), {n4});
    auto n11 = graph.add(new DemoNode(1), {n4});
    auto n12 = graph.add(new DemoNode(1), {n4});

    auto n13 = graph.add(new DemoNode(2), {n5, n6});
    auto n14 = graph.add(new DemoNode(2), {n6, n7});
    auto n15 = graph.add(new DemoNode(2), {n8, n9});
    auto n16 = graph.add(new DemoNode(4), {n10, n11, n12, n13});
    auto n17 = graph.add(new DemoNode(2), {n14, n15});


    graph.init();

    auto edgeWeights = graph.edgeWeights();

    vector<uint32_t> expectEdgeWeight{
            0, 0, 0, 0, 3, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
    };
    for (uint32_t i = 0; i < 18 * 18; ++i) {
        EXPECT_EQ(edgeWeights[i], expectEdgeWeight[i]);
    }

    auto dest = graph.destination();
    auto expect_dest = vector<uint32_t>{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1};
    for (uint32_t i = 0; i < 18; ++i) {
        EXPECT_EQ(dest[i], expect_dest[i]);
    }

    EXPECT_EQ(graph.getExecutor()->pool_size(),9);
    EXPECT_EQ(graph.num_dest(),2);
}

TEST(ExecutionGraphTest, Execute) {
    ExecutionGraph graph;

    auto n0 = graph.add(new DemoSource(4), {});
    auto n1 = graph.add(new DemoSource(5), {});
    auto n2 = graph.add(new DemoSource(7), {});
    auto n3 = graph.add(new DemoSource(8), {});

    auto n4 = graph.add(new DemoNode(1), {n0});
    auto n5 = graph.add(new DemoNode(1), {n0});

    auto n6 = graph.add(new DemoNode(2), {n1, n2});

    auto n7 = graph.add(new DemoNode(1), {n3});
    auto n8 = graph.add(new DemoNode(1), {n3});
    auto n9 = graph.add(new DemoNode(1), {n3});

    auto n10 = graph.add(new DemoNode(1), {n4});
    auto n11 = graph.add(new DemoNode(1), {n4});
    auto n12 = graph.add(new DemoNode(1), {n4});

    auto n13 = graph.add(new DemoNode(2), {n5, n6});
    auto n14 = graph.add(new DemoNode(2), {n6, n7});
    auto n15 = graph.add(new DemoNode(2), {n8, n9});
    auto n16 = graph.add(new DemoNode(4), {n10, n11, n12, n13});
    auto n17 = graph.add(new DemoNode(2), {n14, n15});

    graph.execute();

    auto output1 = static_cast<IntOutput *>(graph.result(n16));
    auto output2 = static_cast<IntOutput *>(graph.result(n17));

    EXPECT_EQ(28, output1->get());
    EXPECT_EQ(36, output2->get());


}