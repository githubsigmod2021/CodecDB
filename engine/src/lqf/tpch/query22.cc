//
// Created by harper on 4/6/20.
//

#include <parquet/types.h>
#include <lqf/data_model.h>
#include <lqf/filter.h>
#include <lqf/agg.h>
#include <lqf/sort.h>
#include <lqf/print.h>
#include <lqf/join.h>
#include <lqf/mat.h>
#include "tpchquery.h"


namespace lqf {
    namespace tpch {

        using namespace agg;
        namespace q22 {
            unordered_set<string> phones{"31", "13", "23", "29", "30", "18", "17"};

            class PositiveAvg : public DoubleAvg {
            public:
                PositiveAvg() : DoubleAvg(Customer::ACCTBAL) {}

                void reduce(DataRow &row) override {
                    if (row[Customer::ACCTBAL].asDouble() > 0) {
                        DoubleAvg::reduce(row);
                    }
                }
            };

            void phone_snapshot(DataRow &to, DataRow &from) {
                auto &phone = from[Customer::PHONE].asByteArray();
                to[0] = (int) ((phone.ptr[0] - '0') * 10 + (phone.ptr[1] - '0'));
                to[1] = from[Customer::ACCTBAL];
            }

            class PhoneBuilder : public RowBuilder {
            public:
                PhoneBuilder() : RowBuilder({JR(Customer::ACCTBAL), JR(Customer::ACCTBAL)}, false, false) {}

                void init() override {
                    RowBuilder::init();
                    snapshoter_ = RowCopyFactory().from(right_type_)->to(RAW)
                            ->to_layout(colOffset(2))
                            ->process(phone_snapshot)
                            ->buildSnapshot();
                }
            };

            class AvgAgg : public NestedNode {
            public:
                AvgAgg(SimpleAgg *inner) : NestedNode(inner, 1) {}

                unique_ptr<NodeOutput> execute(const vector<NodeOutput *> &input) override {
                    auto inneragg = dynamic_cast<SimpleAgg *>(inner_.get());
                    auto input0 = static_cast<TableOutput *>(input[0]);
                    auto result = inneragg->agg(*(input0->get()));
                    double avg = (*(*result->blocks()->collect())[0]->rows())[0][0].asDouble();
                    return unique_ptr<TypedOutput<double>>(new TypedOutput(avg));
                }
            };

            class AvgFilter : public NestedNode {
            public:
                AvgFilter(ColFilter *inner) : NestedNode(inner, 2) {}

                unique_ptr<NodeOutput> execute(const vector<NodeOutput *> &input) override {
                    auto avg = static_cast<TypedOutput<double> *>(input[0])->get();

                    auto innerfilter = dynamic_cast<ColFilter *>(inner_.get());
                    auto pred = dynamic_cast<SimplePredicate *>(innerfilter->predicate(0));

                    pred->predicate([=](const DataField &field) {
                        return field.asDouble() > avg;
                    });

                    auto input0 = static_cast<TableOutput *>(input[1]);
                    auto result = innerfilter->filter(*(input0->get()));
                    return unique_ptr<TableOutput>(new TableOutput(result));
                }
            };
        }

        using namespace q22;

        void executeQ22Debug() {
            auto customer = ParquetTable::Open(Customer::path, {Customer::PHONE, Customer::ACCTBAL, Customer::CUSTKEY});
            customer->blocks()->foreach([](const shared_ptr<Block> &block) {
                auto phonecol = block->col(Customer::PHONE);
                auto block_size = block->size();
                for (uint32_t i = 0; i < block_size; ++i) {
                    auto field = phonecol->next();
                    if (i == 110381) {
                        cout << field << endl;
                    }
                }
            });
        }

        void executeQ22() {
            ExecutionGraph graph;
            auto customer = graph.add(new TableNode(
                    ParquetTable::Open(Customer::path, {Customer::PHONE, Customer::ACCTBAL, Customer::CUSTKEY})), {});
            auto order = graph.add(new TableNode(ParquetTable::Open(Orders::path, {Orders::ORDERKEY, Orders::CUSTKEY})),
                                   {});

            auto custFilter = graph.add(new ColFilter(new SimplePredicate(Customer::PHONE, [](const DataField &field) {
                ByteArray &val = field.asByteArray();
                return phones.find(string((const char *) val.ptr, 2)) != phones.end();
            })), {customer});
            auto matCust = graph.add(new FilterMat(), {custFilter});
//            auto validCust = FilterMat().mat(*custFilter.filter(*customer));

            auto avgagg = graph.add(new AvgAgg(
                    new SimpleAgg([]() { return vector<AggField *>{new PositiveAvg()}; })),
                                    {matCust});
//            auto avgCust = avgagg.agg(*validCust);
//            double avg = (*(*avgCust->blocks()->collect())[0]).rows()->next()[0].asDouble();


            auto avgFilter = graph.add(
                    new AvgFilter(new ColFilter(new SimplePredicate(Customer::ACCTBAL, [=](const DataField &field) {
                        return field.asDouble() > 0;
                    }))), {avgagg, matCust});
//            auto filteredCust = avgFilter.filter(*validCust);
//            cout << filteredCust->size() << endl;

            auto notExistJoin = graph.add(new HashNotExistJoin(Orders::CUSTKEY, 0, new PhoneBuilder()),
                                          {order, avgFilter});
            // PHONE, ACCTBAL
//            auto noorderCust = notExistJoin.join(*order, *filteredCust);

            auto agg = graph.add(new HashAgg(
                    COL_HASHER(0),
                    RowCopyFactory().field(F_REGULAR, 0, 0)->buildSnapshot(),
                    []() { return vector<AggField *>{new DoubleSum(1), new Count()}; }), {notExistJoin});
//            auto result = agg.agg(*noorderCust);

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return SILE(0);
            };
            auto sorter = graph.add(new SmallSort(comparator), {agg});
//            auto sorted = sorter.sort(*result);

            graph.add(new Printer(PBEGIN PI(0) PD(1) PI(2) PEND), {sorter});
//            printer.print(*sorted);
            graph.execute();
        }

        void executeQ22_Backup() {
            auto customer = ParquetTable::Open(Customer::path, {Customer::PHONE, Customer::ACCTBAL, Customer::CUSTKEY});
            auto order = ParquetTable::Open(Orders::path, {Orders::ORDERKEY, Orders::CUSTKEY});

            ColFilter custFilter(new SimplePredicate(Customer::PHONE, [](const DataField &field) {
                ByteArray &val = field.asByteArray();
                return phones.find(string((const char *) val.ptr, 2)) != phones.end();
            }));
            auto validCust = FilterMat().mat(*custFilter.filter(*customer));

            SimpleAgg avgagg([]() { return vector<AggField *>{new PositiveAvg()}; });
            auto avgCust = avgagg.agg(*validCust);
            double avg = (*(*avgCust->blocks()->collect())[0]).rows()->next()[0].asDouble();

            ColFilter avgFilter(new SimplePredicate(Customer::ACCTBAL, [=](const DataField &field) {
                return field.asDouble() > avg;
            }));
            auto filteredCust = avgFilter.filter(*validCust);
//            cout << filteredCust->size() << endl;

            HashNotExistJoin notExistJoin(Orders::CUSTKEY, 0, new PhoneBuilder());
            // PHONE_PREFIX, ACCTBAL
            auto noorderCust = notExistJoin.join(*order, *filteredCust);

            HashAgg agg(COL_HASHER(0),
                        RowCopyFactory().field(F_REGULAR, 0, 0)->buildSnapshot(),
                        []() { return vector<AggField *>{new DoubleSum(1), new Count()}; });
            auto result = agg.agg(*noorderCust);

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return SILE(0);
            };
            SmallSort sorter(comparator);
            auto sorted = sorter.sort(*result);

            Printer printer(PBEGIN PI(0) PD(1) PI(2) PEND);
            printer.print(*sorted);
        }
    }
}