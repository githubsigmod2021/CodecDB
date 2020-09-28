//
// Created by Harper on 4/29/20.
//

#include <functional>
#include <cstring>
#include "parallel.h"

namespace lqf {
    using namespace concurrent;
    namespace parallel {

        Node::Node(const uint32_t num_input, const bool trivial)
                : num_input_(num_input), ready_counter_(0), trivial_(trivial) {}

        bool Node::feed() {
            auto current_count = ++ready_counter_;
            triggered_ = current_count == num_input_;
            return triggered_;
        }

        NestedNode::NestedNode(Node *inner, uint32_t num_input, bool trivial)
                : Node(num_input, trivial), inner_(unique_ptr<Node>(inner)) {}

        ExecutionGraph::~ExecutionGraph() {
            delete done_;
        }

        uint32_t ExecutionGraph::add(Node *target, initializer_list<uint32_t> upstream) {
            auto index = nodes_.size();
            target->index_ = index;
            nodes_.push_back(unique_ptr<Node>(target));

            auto up = new vector<Node *>();
            if (upstream.size() == 0) {
                sources_.push_back(target);
            } else {
                for (auto &ups:upstream) {
                    up->push_back(nodes_[ups].get());
                }
            }
            upstream_.push_back(unique_ptr<vector<Node *>>(up));
            downstream_.push_back(unique_ptr<vector<Node *>>(new vector<Node *>()));
            for (auto &u:upstream) {
                downstream_[u]->push_back(target);
            }
            return index;
        }

        void ExecutionGraph::executeNode(Node *next) {
            if (concurrent_ && !next->trivial()) {
                executeNodeAsync(next);
            } else {
                executeNodeSync(next);
            }
        }

        void ExecutionGraph::executeNodeSync(Node *node) {
            vector<NodeOutput *> inputs;
            auto inputNodes = *(upstream_[node->index_]);
            for (auto &input: inputNodes) {
                inputs.push_back(results_[input->index_].get());
            }
            results_[node->index_] = node->execute(inputs);

            if (destinations_[node->index_]) {
                done_->notify();
            } else {
                // Notify the descendants of readiness
                auto ds = *(downstream_[node->index_]);
                for (auto &next: ds) {
                    if (next->feed()) {
                        executeNode(next);
                    }
                }
            }
        }

        void ExecutionGraph::executeNodeAsync(Node *node) {
            function<void()> task = std::bind(&ExecutionGraph::executeNodeSync, this, node);
            executor_->submit(task);
        }

        void ExecutionGraph::buildFlow() {
            const auto length = nodes_.size();

            edge_weight_.resize(length * length);
            memset(edge_weight_.data(), 0, length * length * sizeof(uint32_t));

            // Initialize all edges to have weight 1
            for (uint32_t i = 0; i < length; ++i) {
                auto &down = *(downstream_[i]);
                for (auto &d: down) {
                    edge_weight_[i * length + d->index_] = 1;
                }
            }
            // Loop all nodes to make sure in-flow equals to out-flow
            uint32_t index = 0;
            while (index < length) {
                uint32_t inflow = 0;
                uint32_t outflow = 0;
                uint32_t first_in = INT32_MAX;
                uint32_t first_out = INT32_MAX;
                for (uint32_t i = 0; i < length; ++i) {
                    inflow += edge_weight_[i * length + index];
                    outflow += edge_weight_[index * length + i];
                    if (first_in == INT32_MAX && edge_weight_[i * length + index] != 0) {
                        first_in = i;
                    }
                    if (first_out == INT32_MAX && edge_weight_[index * length + i] != 0) {
                        first_out = i;
                    }
                }
                if (inflow == 0) { // No input
                    ++index;
                    continue;
                }
                if (outflow == 0) {// No output
                    ++index;
                    continue;
                }
                if (inflow != outflow) { // Unbalanced node
                    if (inflow > outflow) {
                        edge_weight_[index * length + first_out] += inflow - outflow;
                        ++index;
                    } else {
                        edge_weight_[first_in * length + index] += outflow - inflow;
                        index = first_in;
                    }
                } else {
                    ++index;
                }
            }
        }

        void ExecutionGraph::init() {
            // Scan the graph to build max flow
            buildFlow();

            auto length = nodes_.size();

            num_dest_ = 0;
            destinations_.resize(length);
            memset(destinations_.data(), 0, sizeof(uint8_t) * length);

            uint32_t outbound[length];
            uint32_t inbound[length];
            memset(inbound, 0, sizeof(uint32_t) * length);
            memset(outbound, 0, sizeof(uint32_t) * length);

            uint32_t max_flow = 0;
            for (uint32_t i = 0; i < length; ++i) {
                for (uint32_t j = 0; j < length; ++j) {
                    outbound[i] += edge_weight_[i * length + j];
                    inbound[i] += edge_weight_[j * length + i];
                }
                if (outbound[i] == 0) {
                    destinations_[i] = true;
                    ++num_dest_;
                    max_flow += inbound[i];
                }
            }

            executor_ = Executor::Make(max_flow);

            results_.resize(nodes_.size());

            done_ = new Semaphore();
        }

        void ExecutionGraph::execute(bool concurrent) {
            concurrent_ = concurrent;
            init();

            for (auto p: sources_) {
                executeNode(p);
            }
            // Wait for all destinations to be done
            done_->wait(num_dest_);
        }

        NodeOutput *ExecutionGraph::result(uint32_t index) {
            return results_[index].get();
        }
    }
}