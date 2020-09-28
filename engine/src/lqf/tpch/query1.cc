//
// Created by harper on 3/3/20.
//
#include <iostream>
#include <chrono>
#include <parquet/types.h>
#include <lqf/data_model.h>
#include <lqf/filter.h>
#include <lqf/agg.h>
#include <lqf/sort.h>
#include <lqf/print.h>
#include "tpchquery.h"

using namespace std;
using namespace parquet;
using namespace lqf::sboost;
using namespace lqf::agg;

namespace lqf {
    namespace tpch {
        namespace q1 {
            ByteArray dateFrom("1998-09-01");

            class Field3 : public DoubleSum {
            public:
                Field3() : DoubleSum(0) {}

                void reduce(DataRow &dataRow) {
                    value_ = value_.asDouble() + dataRow[LineItem::EXTENDEDPRICE].asDouble()
                                                 * (1 - dataRow[LineItem::DISCOUNT].asDouble());
                }
            };

            class Field4 : public DoubleSum {
            public:
                Field4() : DoubleSum(0) {}

                void reduce(DataRow &dataRow) {
                    value_ = value_.asDouble() + dataRow[LineItem::EXTENDEDPRICE].asDouble()
                                                 * (1 - dataRow[LineItem::DISCOUNT].asDouble()) *
                                                 (1 + dataRow[LineItem::TAX].asDouble());
                }
            };

        }

        using namespace q1;

        void executeQ1Backup() {
            auto lineItemTable = ParquetTable::Open(LineItem::path,
                                                    {LineItem::SHIPDATE, LineItem::QUANTITY, LineItem::EXTENDEDPRICE,
                                                     LineItem::DISCOUNT, LineItem::TAX, LineItem::RETURNFLAG,
                                                     LineItem::LINESTATUS});


            // Use SBoost Filter
            ColFilter colFilter(
                    {new SboostPredicate<ByteArrayType>(LineItem::SHIPDATE, bind(ByteArrayDictLess::build, dateFrom))});
            auto filtered = colFilter.filter(*lineItemTable);

            function<uint64_t(DataRow &row)> indexer = [](DataRow &row) {
                return (row(LineItem::RETURNFLAG).asInt() << 1) + row(LineItem::LINESTATUS).asInt();
            };

            function<vector<AggField *>()> aggFields = []() {
                return vector<AggField *>{
                        new IntSum(LineItem::QUANTITY),
                        new DoubleSum(LineItem::EXTENDEDPRICE),
                        new Field3(),
                        new Field4(),
                        new IntAvg(LineItem::QUANTITY),
                        new DoubleAvg(LineItem::EXTENDEDPRICE),
                        new DoubleAvg(LineItem::DISCOUNT),
                        new Count()
                };
            };

            TableAgg agg(8, indexer, RowCopyFactory().field(F_RAW, LineItem::RETURNFLAG, 0)
                                 ->field(F_RAW, LineItem::LINESTATUS, 1)->buildSnapshot(),
                         aggFields);
            auto agged = agg.agg(*filtered);
//
            SmallSort sort(SORTER2(0, 1));
            auto sorted = sort.sort(*agged);

            auto dict1 = lineItemTable->LoadDictionary<ByteArrayType>(LineItem::RETURNFLAG);
            auto dict2 = lineItemTable->LoadDictionary<ByteArrayType>(LineItem::LINESTATUS);
            auto pdict1 = dict1.get();
            auto pdict2 = dict2.get();

            Printer printer(PBEGIN PDICT(pdict1, 0)
                    PDICT(pdict2, 1)
                    PI(2)
                    PD(3)
                    PD(4)
                    PD(5)
                    PD(6)
                    PD(7)
                    PD(8)
                    PI(9)
            PEND);
            printer.print(*sorted);
        }

        using namespace parallel;

        void executeQ1_good() {
            ExecutionGraph graph;

            auto lineItem = ParquetTable::Open(LineItem::path,
                                               {LineItem::SHIPDATE, LineItem::QUANTITY, LineItem::EXTENDEDPRICE,
                                                LineItem::DISCOUNT, LineItem::TAX, LineItem::RETURNFLAG,
                                                LineItem::LINESTATUS});
            auto lineitemTable = graph.add(new TableNode(lineItem), {});

            // Use SBoost Filter
            auto colFilter = graph.add(new ColFilter(
                    new SboostPredicate<ByteArrayType>(LineItem::SHIPDATE, bind(ByteArrayDictLess::build, dateFrom))),
                                       {lineitemTable});

            function<uint32_t(DataRow &row)> indexer = [](DataRow &row) {
                return (row(LineItem::RETURNFLAG).asInt() << 1) | row(LineItem::LINESTATUS).asInt();
            };
            function<vector<AggField *>()> aggFields = []() {
                return vector<AggField *>{
                        new IntSum(LineItem::QUANTITY),
                        new DoubleSum(LineItem::EXTENDEDPRICE),
                        new Field3(),
                        new Field4(),
                        new IntAvg(LineItem::QUANTITY),
                        new DoubleAvg(LineItem::EXTENDEDPRICE),
                        new DoubleAvg(LineItem::DISCOUNT),
                        new Count()
                };
            };

            auto agg = graph.add(new TableAgg(8, indexer, RowCopyFactory().field(F_RAW, LineItem::RETURNFLAG, 0)
                                                      ->field(F_RAW, LineItem::LINESTATUS, 1)->buildSnapshot(),
                                              aggFields), {colFilter});

            auto sort = graph.add(new SmallSort(SORTER2(0, 1)), {agg});

            auto dict1 = lineItem->LoadDictionary<ByteArrayType>(LineItem::RETURNFLAG);
            auto dict2 = lineItem->LoadDictionary<ByteArrayType>(LineItem::LINESTATUS);
            auto pdict1 = dict1.get();
            auto pdict2 = dict2.get();

            graph.add(new Printer(PBEGIN PDICT(pdict1, 0)
                    PDICT(pdict2, 1)
                    PI(2)
                    PD(3)
                    PD(4)
                    PD(5)
                    PD(6)
                    PD(7)
                    PD(8)
                    PI(9)
            PEND), {sort});

            graph.execute(true);
        }

        void executeQ1() {
            ExecutionGraph graph;

            auto lineItem = ParquetTable::Open(LineItem::path,
                                               {LineItem::SHIPDATE, LineItem::QUANTITY, LineItem::EXTENDEDPRICE,
                                                LineItem::DISCOUNT, LineItem::TAX, LineItem::RETURNFLAG,
                                                LineItem::LINESTATUS});
            auto lineitemTable = graph.add(new TableNode(lineItem), {});

            // Use SBoost Filter
            auto colFilter = graph.add(new ColFilter(
                    new SimplePredicate(LineItem::SHIPDATE,
                                        [](const DataField &field) { return field.asByteArray() < dateFrom; })),
                                       {lineitemTable});

            function<uint32_t(DataRow &row)> indexer = [](DataRow &row) {
                return (row(LineItem::RETURNFLAG).asInt() << 1) | row(LineItem::LINESTATUS).asInt();
            };
            function<vector<AggField *>()> aggFields = []() {
                return vector<AggField *>{
                        new IntSum(LineItem::QUANTITY),
                        new DoubleSum(LineItem::EXTENDEDPRICE),
                        new Field3(),
                        new Field4(),
                        new IntAvg(LineItem::QUANTITY),
                        new DoubleAvg(LineItem::EXTENDEDPRICE),
                        new DoubleAvg(LineItem::DISCOUNT),
                        new Count()
                };
            };

            auto agg = graph.add(new TableAgg(8, indexer, RowCopyFactory().field(F_RAW, LineItem::RETURNFLAG, 0)
                                                      ->field(F_RAW, LineItem::LINESTATUS, 1)->buildSnapshot(),
                                              aggFields), {colFilter});

            auto sort = graph.add(new SmallSort(SORTER2(0, 1)), {agg});

            auto dict1 = lineItem->LoadDictionary<ByteArrayType>(LineItem::RETURNFLAG);
            auto dict2 = lineItem->LoadDictionary<ByteArrayType>(LineItem::LINESTATUS);
            auto pdict1 = dict1.get();
            auto pdict2 = dict2.get();

            graph.add(new Printer(PBEGIN PDICT(pdict1, 0)
                    PDICT(pdict2, 1)
                    PI(2)
                    PD(3)
                    PD(4)
                    PD(5)
                    PD(6)
                    PD(7)
                    PD(8)
                    PI(9)
            PEND), {sort});

            graph.execute(true);
        }
    }
}
