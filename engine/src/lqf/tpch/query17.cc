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
        namespace q17 {
            ByteArray brand("Brand#23");
            ByteArray container("MED BOX");

            class YearSumField : public DoubleSum {
            public:
                YearSumField() : DoubleSum(0) {}

                void dump() override {
                    value_ = value_.asDouble() / 7;
                }
            };
        }

        using namespace q17;

        void executeQ17_Graph() {
            ExecutionGraph graph;

            auto part = graph.add(
                    new TableNode(ParquetTable::Open(Part::path, {Part::BRAND, Part::CONTAINER, Part::PARTKEY})), {});
            auto lineitem = graph.add(new TableNode(ParquetTable::Open(LineItem::path,
                                                                       {LineItem::PARTKEY, LineItem::QUANTITY,
                                                                        LineItem::EXTENDEDPRICE})), {});

            auto partFilter = graph.add(
                    new ColFilter(
                            {new SboostPredicate<ByteArrayType>(Part::BRAND, bind(&ByteArrayDictEq::build, brand)),
                             new SboostPredicate<ByteArrayType>(Part::CONTAINER,
                                                                bind(&ByteArrayDictEq::build, container))}),
                    {part});

            auto lineitemFilter = graph.add(new FilterJoin(LineItem::PARTKEY, Part::PARTKEY),
                                            {lineitem, partFilter});
            auto lineitemMat = graph.add(new FilterMat(), {lineitemFilter});

            auto avgquantity = graph.add(
                    new HashAgg(COL_HASHER(LineItem::PARTKEY),
                                RowCopyFactory().field(F_REGULAR, LineItem::PARTKEY, 0)->buildSnapshot(),
                                []() { return vector<AggField *>{new IntAvg(LineItem::QUANTITY)}; }),
                    {lineitemMat});
            // PARTKEY, AVG_QTY

            auto withAvgJoin = graph.add(
                    new HashJoin(LineItem::PARTKEY, 0, new RowBuilder({JL(LineItem::EXTENDEDPRICE), JR(1),}),
                                 [](DataRow &left, DataRow &right) {
                                     return left[LineItem::QUANTITY].asInt() < 0.2 * right[0].asDouble();
                                 }), {lineitemMat, avgquantity});

            auto sumagg = graph.add(
                    new SimpleAgg([]() { return vector<AggField *>{new YearSumField()}; }),
                    {withAvgJoin});

            graph.add(new Printer(PBEGIN PD(0) PEND), {sumagg});

            graph.execute();
        }

        void executeQ17() {

            auto part = ParquetTable::Open(Part::path, {Part::BRAND, Part::CONTAINER, Part::PARTKEY});
            auto lineitem = ParquetTable::Open(LineItem::path,
                                               {LineItem::PARTKEY, LineItem::QUANTITY, LineItem::EXTENDEDPRICE});

            ColFilter partFilter({new SboostPredicate<ByteArrayType>(Part::BRAND,
                                                                     bind(&ByteArrayDictEq::build, brand)),
                                  new SboostPredicate<ByteArrayType>(Part::CONTAINER,
                                                                     bind(&ByteArrayDictEq::build, container))});
            auto validPart = partFilter.filter(*part);

            FilterJoin lineitemFilter(LineItem::PARTKEY, Part::PARTKEY);
            auto validlineitem = FilterMat().mat(*lineitemFilter.join(*lineitem, *validPart));

            HashAgg avgquantity(COL_HASHER(LineItem::PARTKEY),
                                                RowCopyFactory().field(F_REGULAR, LineItem::PARTKEY, 0)->buildSnapshot(),
                                                []() { return vector<AggField *>{new IntAvg(LineItem::QUANTITY)}; });
            // PARTKEY, AVG_QTY
            auto aggedlineitem = avgquantity.agg(*validlineitem);

            HashJoin withAvgJoin(LineItem::PARTKEY, 0, new RowBuilder({JL(LineItem::EXTENDEDPRICE), JR(1),}),
                                 [](DataRow &left, DataRow &right) {
                                     return left[LineItem::QUANTITY].asInt() < 0.2 * right[0].asDouble();
                                 });
            auto result = withAvgJoin.join(*validlineitem, *aggedlineitem);


            SimpleAgg sumagg([]() { return vector<AggField *>{new YearSumField()}; });
            result = sumagg.agg(*result);

            Printer printer(PBEGIN PD(0) PEND);
            printer.print(*result);
        }
    }
}