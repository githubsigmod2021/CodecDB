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
#include <lqf/union.h>
#include "tpchquery.h"


namespace lqf {
    namespace tpch {
        using namespace agg;
        using namespace sboost;

        namespace q12 {
            ByteArray highp1("1-URGENT");
            ByteArray highp2("2-HIGH");
            ByteArray dateFrom("1994-01-01");
            ByteArray dateTo("1995-01-01");
            ByteArray mode1("MAIL");
            ByteArray mode2("SHIP");

            class OrderHighPriorityField : public IntSum {
            public:
                OrderHighPriorityField() : IntSum(0) {}

                void reduce(DataRow &input) override {
                    int32_t orderpriority = input[2].asInt();
                    if (orderpriority == 0 || orderpriority == 1) {
                        value_ = value_.asInt() + input[1].asInt();
                    }
                }
            };

            class OrderLowPriorityField : public IntSum {
            public:
                OrderLowPriorityField() : IntSum(0) {}

                void reduce(DataRow &input) override {
                    int32_t orderpriority = input[2].asInt();
                    if (!(orderpriority == 0 || orderpriority == 1)) {
                        value_ = value_.asInt() + input[1].asInt();
                    }
                }
            };
        }
        using namespace q12;

        void executeQ12() {
            ExecutionGraph graph;

            auto lineitem = graph.add(new TableNode(ParquetTable::Open(LineItem::path,
                                                                       {LineItem::RECEIPTDATE, LineItem::SHIPMODE,
                                                                        LineItem::COMMITDATE,
                                                                        LineItem::SHIPDATE, LineItem::ORDERKEY})), {});
            auto order = graph.add(
                    new TableNode(ParquetTable::Open(Orders::path, {Orders::ORDERKEY, Orders::ORDERPRIORITY})), {});

            auto lineitemFilter = graph.add(new ColFilter(
                    {new SboostPredicate<ByteArrayType>(LineItem::RECEIPTDATE,
                                                        bind(&ByteArrayDictRangele::build,
                                                             dateFrom, dateTo)),
                     new SboostPredicate<ByteArrayType>(LineItem::SHIPMODE,
                                                        bind(&ByteArrayDictMultiEq::build,
                                                             [=](const ByteArray &data) {
                                                                 return data == mode1 ||
                                                                        data == mode2;
                                                             }))}), {lineitem});

            auto lineItemAgainFilter = graph.add(new RowFilter([](DataRow &row) {
                auto &commitDate = row[LineItem::COMMITDATE].asByteArray();
                auto &shipDate = row[LineItem::SHIPDATE].asByteArray();
                auto &receiptDate = row[LineItem::RECEIPTDATE].asByteArray();
                return commitDate < receiptDate && shipDate < commitDate;
            }), {lineitemFilter});

            function<uint64_t(DataRow &)> hasher = [](DataRow &row) {
                return (row[LineItem::ORDERKEY].asInt() << 3) + row(LineItem::SHIPMODE).asInt();
            };

            auto lineitemAgg_obj = new HashAgg(
                    hasher,
                    RowCopyFactory().field(F_REGULAR, LineItem::ORDERKEY, 0)
                            ->field(F_RAW, LineItem::SHIPMODE, 1)->buildSnapshot(),
                    []() { return vector<AggField *>({new agg::Count()}); }, nullptr, true);
            auto lineitemAgg = graph.add(lineitemAgg_obj, {lineItemAgainFilter});
            // ORDER_KEY, SHIPMODE, COUNT

            auto orderFilter = graph.add(new FilterJoin(Orders::ORDERKEY, 0), {order, lineitemAgg});

            auto join = graph.add(new HashColumnJoin(0, Orders::ORDERKEY,
                                                     new ColumnBuilder({JL(1), JL(2), JRR(Orders::ORDERPRIORITY)})),
                                  {lineitemAgg, orderFilter});
            // SHIPMODE, COUNT, ORDERPRIORITY

            auto finalAgg = graph.add(
                    new HashAgg(COL_HASHER(0),
                                RowCopyFactory().field(F_REGULAR, 0, 0)->buildSnapshot(), []() {
                                return vector<AggField *>{new OrderHighPriorityField(), new OrderLowPriorityField()};
                            }), {join});

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return SILE(0);
            };
            auto sort = graph.add(new SmallSort(comparator), {finalAgg});

            graph.add(new Printer(PBEGIN PI(0) PI(1) PI(2) PEND), {sort});
            graph.execute();
        }

        void executeQ12_Backup() {
            auto lineitem = ParquetTable::Open(LineItem::path,
                                               {LineItem::RECEIPTDATE, LineItem::SHIPMODE, LineItem::COMMITDATE,
                                                LineItem::SHIPDATE, LineItem::ORDERKEY});
            auto order = ParquetTable::Open(Orders::path, {Orders::ORDERKEY, Orders::ORDERPRIORITY});

            ColFilter lineitemFilter({new SboostPredicate<ByteArrayType>(LineItem::RECEIPTDATE,
                                                                         bind(&ByteArrayDictRangele::build,
                                                                              dateFrom, dateTo)),
                                      new SboostPredicate<ByteArrayType>(LineItem::SHIPMODE,
                                                                         bind(&ByteArrayDictMultiEq::build,
                                                                              [=](const ByteArray &data) {
                                                                                  return data == mode1 ||
                                                                                         data == mode2;
                                                                              }))});
            auto validLineitem = lineitemFilter.filter(*lineitem);

            SboostRow2Filter lineItemAgainFilter(LineItem::SHIPDATE, LineItem::COMMITDATE, LineItem::RECEIPTDATE);
//
//            [](DataRow &row) {
//                auto &commitDate = row[LineItem::COMMITDATE].asByteArray();
//                auto &shipDate = row[LineItem::SHIPDATE].asByteArray();
//                auto &receiptDate = row[LineItem::RECEIPTDATE].asByteArray();
//                return commitDate < receiptDate && shipDate < commitDate;
//            });
            auto validLineitem2 = lineItemAgainFilter.filter(*lineitem);

            FilterAnd fand(2);
            auto validDateLineitem = fand.execute(vector<Table *>{validLineitem.get(), validLineitem2.get()});

            function<uint64_t(DataRow &)> hasher = [](DataRow &row) {
                return (row[LineItem::ORDERKEY].asInt() << 3) + row(LineItem::SHIPMODE).asInt();
            };

            HashAgg lineitemAgg(hasher,
                                RowCopyFactory().field(F_REGULAR, LineItem::ORDERKEY, 0)
                                        ->field(F_RAW, LineItem::SHIPMODE, 1)->buildSnapshot(),
                                []() { return vector<AggField *>({new agg::Count()}); }, nullptr, true);
            // ORDER_KEY, SHIPMODE, COUNT
            auto agglineitem = lineitemAgg.agg(*validDateLineitem);

            FilterJoin orderFilterJoin(Orders::ORDERKEY, 0);
            auto filteredOrder = orderFilterJoin.join(*order, *agglineitem);

            HashColumnJoin join(0, Orders::ORDERKEY, new ColumnBuilder({JL(1), JL(2), JRR(Orders::ORDERPRIORITY)}));
            // SHIPMODE, COUNT, ORDERPRIORITY
            auto itemwithorder = join.join(*agglineitem, *filteredOrder);

            HashAgg finalAgg(COL_HASHER(0),
                             RowCopyFactory().field(F_REGULAR, 0, 0)->buildSnapshot(),
                             []() {
                                 return vector<AggField *>{new OrderHighPriorityField(),
                                                           new OrderLowPriorityField()};
                             });
            auto result = finalAgg.agg(*itemwithorder);

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return SILE(0);
            };
            SmallSort sorter(comparator);
            auto sorted = sorter.sort(*result);

            Printer printer(PBEGIN PI(0) PI(1) PI(2) PEND);
            printer.print(*sorted);
        }
    }
}