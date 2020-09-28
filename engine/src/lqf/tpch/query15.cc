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
        using namespace sboost;

        namespace q15 {

            ByteArray dateFrom("1996-01-01");
            ByteArray dateTo("1996-04-01");

            class PriceField : public DoubleSum {
            public:
                PriceField() : DoubleSum(0) {}

                void reduce(DataRow &input) override {
                    value_ = value_.asInt() +
                             input[LineItem::EXTENDEDPRICE].asDouble() * (1 - input[LineItem::DISCOUNT].asDouble());
                }
            };
        }
        using namespace q15;
        using namespace agg::recording;

        void executeQ15() {
            ExecutionGraph graph;

            auto lineitem = graph.add(new TableNode(
                    ParquetTable::Open(LineItem::path, {LineItem::SUPPKEY, LineItem::SHIPDATE, LineItem::EXTENDEDPRICE,
                                                        LineItem::DISCOUNT})), {});
            auto supplier = graph.add(new TableNode(
                    ParquetTable::Open(Supplier::path, {Supplier::SUPPKEY, Supplier::NAME, Supplier::ADDRESS,
                                                        Supplier::PHONE})), {});

            auto lineitemDateFilter = graph.add(
                    new ColFilter(new SboostPredicate<ByteArrayType>(LineItem::SHIPDATE,
                                                                     bind(&ByteArrayDictRangele::build, dateFrom,
                                                                          dateTo))), {lineitem});

            auto suppkeyAgg = graph.add(new HashLargeAgg(
                    COL_HASHER(LineItem::SUPPKEY),
                    RowCopyFactory().field(F_REGULAR, LineItem::SUPPKEY, 0)->buildSnapshot(),
                    []() { return vector<AggField *>{new PriceField()}; }),
                                        {lineitemDateFilter});
            // SUPPKEY, REVENUE

            auto maxAgg_obj = new RecordingSimpleAgg([]() { return new RecordingDoubleMax(1, 0); });
            auto maxAgg = graph.add(maxAgg_obj, {suppkeyAgg});
            // REV, SUPPKEY

            auto join = graph.add(
                    new HashJoin(Supplier::SUPPKEY, 1,
                                 new RowBuilder(
                                         {JLS(Supplier::NAME), JLS(Supplier::ADDRESS), JLS(Supplier::PHONE), JR(0)},
                                         true, false)), {supplier, maxAgg});

            auto sort = graph.add(new SmallSort([](DataRow *a, DataRow *b) { return SILE(0); }), {join});

            graph.add(new Printer(PBEGIN PI(0) PB(1) PB(2) PB(3) PD(4) PEND), {sort});

            graph.execute();
        }

        void executeQ15Backup() {

            auto lineitem = ParquetTable::Open(LineItem::path,
                                               {LineItem::SUPPKEY, LineItem::SHIPDATE, LineItem::EXTENDEDPRICE,
                                                LineItem::DISCOUNT});
            auto supplier = ParquetTable::Open(Supplier::path, {Supplier::SUPPKEY, Supplier::NAME, Supplier::ADDRESS,
                                                                Supplier::PHONE});

            ColFilter lineitemDateFilter({new SboostPredicate<ByteArrayType>(LineItem::SHIPDATE,
                                                                             bind(&ByteArrayDictRangele::build,
                                                                                  dateFrom, dateTo))});
            auto filteredLineitem = lineitemDateFilter.filter(*lineitem);


            HashLargeAgg suppkeyAgg(COL_HASHER(LineItem::SUPPKEY),
                               RowCopyFactory().field(F_REGULAR, LineItem::SUPPKEY, 0)->buildSnapshot(),
                               []() { return vector<AggField *>{new PriceField()}; });
            // SUPPKEY, REVENUE
            auto revenueView = suppkeyAgg.agg(*filteredLineitem);

            RecordingSimpleAgg maxAgg([]() { return new RecordingDoubleMax(1, 0); });
            // REV, SUPPKEY
            auto maxRevenue = maxAgg.agg(*revenueView);

            HashJoin join(Supplier::SUPPKEY, 1, new RowBuilder({JLS(Supplier::NAME), JLS(Supplier::ADDRESS),
                                                                JLS(Supplier::PHONE), JR(0)}, true, false));
            auto supplierWithRev = join.join(*supplier, *maxRevenue);

            SmallSort sort([](DataRow *a, DataRow *b) { return SILE(0); });
            auto sorted = sort.sort(*supplierWithRev);

            Printer printer(PBEGIN PI(0) PB(1) PB(2) PB(3) PD(4) PEND);
            printer.print(*sorted);
        }
    }
}