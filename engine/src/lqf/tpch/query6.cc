//
// Created by harper on 4/4/20.
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
        using namespace sboost;
        using namespace agg;
        namespace q6 {
            int quantity = 24;
            ByteArray dateFrom("1994-01-01");
            ByteArray dateTo("1995-01-01");
            double discountFrom = 0.04;
            double discountTo = 0.06;

            class PriceField : public DoubleSum {
            public:
                PriceField() : DoubleSum(0) {}

                void reduce(DataRow &input) override {
                    value_ = value_.asDouble() +
                             input[LineItem::EXTENDEDPRICE].asDouble() * input[LineItem::DISCOUNT].asDouble();
                }
            };
        }
        using namespace q6;

        void executeQ6() {
            auto lineitemTable = ParquetTable::Open(LineItem::path,
                                                    {LineItem::SHIPDATE, LineItem::DISCOUNT, LineItem::QUANTITY,
                                                     LineItem::EXTENDEDPRICE});
            ExecutionGraph graph;

            auto lineitem = graph.add(new TableNode(lineitemTable), {});

            auto filter = graph.add(new ColFilter({new SboostPredicate<ByteArrayType>(LineItem::SHIPDATE,
                                                                                      bind(&ByteArrayDictRangele::build,
                                                                                           dateFrom,
                                                                                           dateTo)),
                                                   new SboostPredicate<DoubleType>(LineItem::DISCOUNT,
                                                                                   bind(&DoubleDictBetween::build,
                                                                                        discountFrom,
                                                                                        discountTo)),
                                                   new SboostPredicate<Int32Type>(LineItem::QUANTITY,
                                                                                  bind(&Int32DictLess::build, quantity))
                                                  }), {lineitem});

            auto agg = graph.add(new SimpleAgg([]() { return vector<AggField *>({new PriceField()}); }), {filter});

            graph.add(new Printer(PBEGIN PD(0) PEND), {agg});
            graph.execute();
        }

        void executeQ6Backup() {
            auto lineitemTable = ParquetTable::Open(LineItem::path,
                                                    {LineItem::SHIPDATE, LineItem::DISCOUNT, LineItem::QUANTITY,
                                                     LineItem::EXTENDEDPRICE});
            ColFilter filter({new SboostPredicate<ByteArrayType>(LineItem::SHIPDATE,
                                                                 bind(&ByteArrayDictRangele::build, dateFrom,
                                                                      dateTo)),
                              new SboostPredicate<DoubleType>(LineItem::DISCOUNT,
                                                              bind(&DoubleDictBetween::build, discountFrom,
                                                                   discountTo)),
                              new SboostPredicate<Int32Type>(LineItem::QUANTITY,
                                                             bind(&Int32DictLess::build, quantity))
                             });
            auto filtered = filter.filter(*lineitemTable);

            SimpleAgg agg([]() { return vector<AggField *>({new PriceField()}); });
            auto agged = agg.agg(*filtered);

            Printer printer(PBEGIN PD(0) PEND);
            printer.print(*agged);
        }
    }
}