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
#include <lqf/util.h>
#include "tpchquery.h"


namespace lqf {
    namespace tpch {
        using namespace agg;
        using namespace sboost;
        namespace q14 {

            ByteArray dateFrom("1995-09-01");
            ByteArray dateTo("1995-10-01");
            const char *prefix = "PROMO";

            void compute_match(DataRow &target, DataRow &source) {
                target[0] = source[LineItem::EXTENDEDPRICE].asDouble() * (1 - source[LineItem::DISCOUNT].asDouble());
                target[1] = target[0].asDouble();
            }

            void compute_unmatch(DataRow &target, DataRow &source) {
                target[0] = source[LineItem::EXTENDEDPRICE].asDouble() * (1 - source[LineItem::DISCOUNT].asDouble());
                target[1] = 0;
            }
        }

        using namespace q14;

        void executeQ14() {
            ExecutionGraph graph;
            auto lineitem = graph.add(new TableNode(ParquetTable::Open(LineItem::path,
                                                                       {LineItem::SHIPDATE, LineItem::PARTKEY,
                                                                        LineItem::EXTENDEDPRICE,
                                                                        LineItem::DISCOUNT})), {});
            auto part = graph.add(new TableNode(ParquetTable::Open(Part::path, {Part::PARTKEY, Part::TYPE})), {});

            function<bool(const ByteArray &)> filter = [](const ByteArray &val) {
                const char *begin = (const char *) val.ptr;
                return lqf::util::strnstr(begin, prefix, val.len) == begin;
            };
            auto partFilter = graph.add(
                    new ColFilter(new SboostPredicate<ByteArrayType>(
                            Part::TYPE, bind(&ByteArrayDictMultiEq::build, filter))), {part});

            auto lineitemShipdateFilter = graph.add(
                    new ColFilter(new SboostPredicate<ByteArrayType>(
                            LineItem::SHIPDATE, bind(&ByteArrayDictRangele::build, dateFrom, dateTo))),
                    {lineitem});

            auto join_obj = new FilterTransformJoin(
                    LineItem::PARTKEY, Part::PARTKEY,
                    RowCopyFactory().from(EXTERNAL)->to(RAW)->to_layout(colOffset(2))
                            ->process(compute_match)->buildSnapshot(),
                    RowCopyFactory().from(EXTERNAL)->to(RAW)->to_layout(colOffset(2))
                            ->process(compute_unmatch)->buildSnapshot()
            );
            auto join = graph.add(join_obj, {lineitemShipdateFilter, partFilter});

            auto simpleAgg = graph.add(new SimpleAgg(
                    []() { return vector<AggField *>{new DoubleSum(0), new DoubleSum(1)}; }), {join});

            graph.add(new Printer(PBEGIN PD(0) PD(1) PEND), {simpleAgg});
            graph.execute();
        }
    }
}