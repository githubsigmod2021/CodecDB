//
// Created by harper on 4/6/20.
//

#include <string.h>
#include <unordered_map>
#include <parquet/types.h>
#include <lqf/data_model.h>
#include <lqf/filter.h>
#include <lqf/agg.h>
#include <lqf/sort.h>
#include <lqf/print.h>
#include <lqf/join.h>
#include <lqf/mat.h>
#include <lqf/util.h>
#include "tpchquery.h"

using namespace std::chrono;
namespace lqf {
    namespace tpch {

        namespace q13 {
            class CustCountBuilder : public RowBuilder {
            public:
                CustCountBuilder() : RowBuilder({JR(1)}, true) {}

                void build(DataRow &target, DataRow &left, DataRow &right, int key) {
                    target[0] = key;
                    if (&right == &MemDataRow::EMPTY) {
                        target[1] = 0;
                    } else {
                        target[1] = right[0];
                    }
                }
            };

            class CustCountAgg : public Node {
            protected:
                int num_stripes_ = 32;

                shared_ptr<vector<shared_ptr<vector<int64_t>>>> makeStripes(const shared_ptr<Block> &block) {
                    shared_ptr<vector<shared_ptr<vector<int64_t>>>> stripes = make_shared<vector<shared_ptr<vector<int64_t>>>>();
                    for (auto i = 0; i < num_stripes_; ++i) {
                        stripes->push_back(make_shared<vector<int64_t>>());
                    }

                    auto block_size = block->size();
                    auto cust = block->col(Orders::CUSTKEY);
                    auto mask = num_stripes_ - 1;

                    for (auto i = 0u; i < block_size; ++i) {
                        auto value = cust->next().asInt();
                        auto index = value & mask;
                        (*stripes)[index]->push_back(value);
                    }
                    return stripes;
                }

                shared_ptr<vector<shared_ptr<unordered_map<int64_t, int64_t>>>>
                processStripes(const shared_ptr<vector<shared_ptr<vector<int64_t>>>> stripes) {
                    auto maps = make_shared<vector<shared_ptr<unordered_map<int64_t, int64_t>>>>();

                    for (auto i = 0; i < num_stripes_; ++i) {
                        auto map = make_shared<unordered_map<int64_t, int64_t>>();
                        maps->push_back(map);
                        auto keys = (*stripes)[i];
                        auto end = map->cend();
                        for (auto &custkey: *keys) {
                            auto found = map->find(custkey);
                            if (found != end) {
                                found->second++;
                            } else {
                                (*map)[custkey] = 1;
                            }
                        }
                    }
                    return maps;
                }

                shared_ptr<vector<shared_ptr<unordered_map<int64_t, int64_t>>>>
                merge(shared_ptr<vector<shared_ptr<unordered_map<int64_t, int64_t>>>> lefts,
                      shared_ptr<vector<shared_ptr<unordered_map<int64_t, int64_t>>>> rights) {
                    for (auto i = 0; i < num_stripes_; ++i) {
                        auto left = (*lefts)[i];
                        auto right = (*rights)[i];
                        auto lend = left->cend();
                        for (auto &entry:*right) {
                            auto found = left->find(entry.first);
                            if (found != lend) {
                                found->second += entry.second;
                            } else {
                                (*left)[entry.first] = entry.second;
                            }
                        }
                    }
                    return lefts;
                }

            public:
                CustCountAgg() : Node(1) {}

                unique_ptr<NodeOutput> execute(const vector<NodeOutput *> &input)
                override {
                    auto input0 = static_cast<TableOutput *>(input[0]);
                    auto result = agg(*(input0->get()));
                    return unique_ptr<TableOutput>(new TableOutput(result));
                }

                shared_ptr<Table> agg(Table &input) {
                    function<shared_ptr<vector<shared_ptr<vector<int64_t>>>>(const shared_ptr<Block> &)> striper =
                            bind(&CustCountAgg::makeStripes, this, _1);
                    function<shared_ptr<vector<shared_ptr<unordered_map<int64_t, int64_t>>>>(
                            const shared_ptr<vector<shared_ptr<vector<int64_t>>>> &)> counter =
                            bind(&CustCountAgg::processStripes, this, _1);
                    auto counted = input.blocks()->map(striper)->map(counter)->reduce(
                            bind(&CustCountAgg::merge, this, _1, _2));

                    auto result = MemTable::Make(2);

                    for (auto i = 0; i < num_stripes_; ++i) {
                        auto stripe = (*counted)[i];
                        auto block = result->allocate(stripe->size());
                        auto rows = block->rows();
                        for (auto &entry: *stripe) {
                            DataRow &next = rows->next();
                            next[0] = (int) entry.first;
                            next[1] = (int) entry.second;
                        }
                    }
                    return result;
                }
            };
        }

        using namespace q13;
        using namespace agg;

        void executeQ13_graph() {
            ExecutionGraph graph;

            auto customer = graph.add(new TableNode(ParquetTable::Open(Customer::path, {Customer::CUSTKEY})), {});
            auto order = graph.add(
                    new TableNode(ParquetTable::Open(Orders::path, {Orders::COMMENT, Orders::CUSTKEY})),
                    {});

            auto orderCommentFilter = graph.add(
                    new ColFilter(new SimplePredicate(Orders::COMMENT, [](const DataField &input) {
                        ByteArray &value = input.asByteArray();
                        const char *begin = (const char *) value.ptr;
                        char *index = lqf::util::strnstr(begin, "special", value.len);
                        if (index != NULL) {
                            return NULL ==
                                   lqf::util::strnstr(index + 7, "requests", value.len - (index - begin) - 7);
                        }
                        return true;
                    })), {order});

            using namespace agg;
            auto orderCustAgg = graph.add(
                    new StripeHashAgg(32, COL_HASHER(Orders::CUSTKEY), COL_HASHER(0),
                                      RowCopyFactory().field(F_REGULAR, Orders::CUSTKEY, 0)->buildSnapshot(),
                                      RowCopyFactory().field(F_REGULAR, 0, 0)->buildSnapshot(),
                                      []() { return vector<AggField *>{new Count()}; }),
                    {orderCommentFilter});
            // CUSTKEY, COUNT

            auto join_obj = new HashJoin(Customer::CUSTKEY, 0, new CustCountBuilder());
            join_obj->useOuter();
            auto join = graph.add(join_obj, {customer, orderCustAgg});
            // CUSTKEY, COUNT

            auto countAgg = graph.add(new HashAgg(COL_HASHER(1),
                                                  RowCopyFactory().field(F_REGULAR, 1, 0)->buildSnapshot(),
                                                  []() { return vector<AggField *>{new agg::Count()}; }),
                                      {join});
            // COUNT, DIST

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return SIGE(1) || (SIE(1) && SIGE(0));
            };

            auto sort = graph.add(new SmallSort(comparator), {countAgg});

            graph.add(new Printer(PBEGIN PI(0) PI(1) PEND), {sort});
            graph.execute();
        }

        void executeQ13() {
            auto customer = ParquetTable::Open(Customer::path, {Customer::CUSTKEY});
            auto order = ParquetTable::Open(Orders::path, {Orders::COMMENT, Orders::CUSTKEY});

            ColFilter orderCommentFilter({new SimplePredicate(Orders::COMMENT, [](const DataField &input) {
                ByteArray &value = input.asByteArray();
                const char *begin = (const char *) value.ptr;
                char *index = lqf::util::strnstr(begin, "special", value.len);
                if (index != NULL) {
                    return NULL == lqf::util::strnstr(index + 7, "requests", value.len - (index - begin) - 7);
                }
                return true;
            })});
            auto validOrder = orderCommentFilter.filter(*order);

//            HashAgg orderCustAgg(COL_HASHER(Orders::CUSTKEY),
//                                 RowCopyFactory().field(F_REGULAR, Orders::CUSTKEY, 0)->buildSnapshot(),
//                                 []() { return vector<AggField *>{new Count()}; });
//            CustCountAgg orderCustAgg;
            StripeHashAgg orderCustAgg(32, COL_HASHER(Orders::CUSTKEY), COL_HASHER(0),
                                       RowCopyFactory().field(F_REGULAR, Orders::CUSTKEY, 0)->buildSnapshot(),
                                       RowCopyFactory().field(F_REGULAR, 0, 0)->buildSnapshot(),
                                       []() { return vector<AggField *>{new Count()}; });
            // CUSTKEY, COUNT
            auto orderCount = orderCustAgg.agg(*validOrder);

            HashJoin join(Customer::CUSTKEY, 0, new CustCountBuilder());
            join.useOuter();
            // CUSTKEY, COUNT
            auto custCount = join.join(*customer, *orderCount);

            HashAgg countAgg(COL_HASHER(1),
                             RowCopyFactory().field(F_REGULAR, 1, 0)->buildSnapshot(),
                             []() { return vector<AggField *>{new Count()}; });
            // COUNT, DIST
            auto result = countAgg.agg(*custCount);

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return SIGE(1) || (SIE(1) && SIGE(0));
            };

            SmallSort sort(comparator);
            auto sorted = sort.sort(*result);

            Printer printer(PBEGIN PI(0) PI(1) PEND);
            printer.print(*sorted);
        }
    }
}