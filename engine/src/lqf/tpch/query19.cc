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
        namespace q19 {
            ByteArray brand1("Brand#12");
            ByteArray brand2("Brand#23");
            ByteArray brand3("Brand#34");

            ByteArray shipmentInst("DELIVER IN PERSON");
            ByteArray shipMode1("AIR");
            ByteArray shipMode2("REG AIR");


            class PriceField : public DoubleSum {
            public:
                PriceField() : DoubleSum(LineItem::EXTENDEDPRICE) {}

                void reduce(DataRow &row) override {
                    value_ = value_.asDouble() +
                             row[LineItem::EXTENDEDPRICE].asDouble() * (1 - row[LineItem::DISCOUNT].asDouble());
                }
            };
        }
        using namespace q19;

        void executeQ19_Graph() {
            ExecutionGraph graph;

            auto lineitem = graph.add(
                    new TableNode(ParquetTable::Open(LineItem::path, {LineItem::PARTKEY, LineItem::QUANTITY,
                                                                      LineItem::SHIPMODE, LineItem::SHIPINSTRUCT,
                                                                      LineItem::EXTENDEDPRICE, LineItem::DISCOUNT})),
                    {});
            auto part = graph.add(new TableNode(
                    ParquetTable::Open(Part::path, {Part::PARTKEY, Part::BRAND, Part::CONTAINER, Part::SIZE})), {});

            auto lineitemBaseFilter = graph.add(
                    new ColFilter({new SboostPredicate<ByteArrayType>(LineItem::SHIPINSTRUCT,
                                                                      bind(&ByteArrayDictEq::build,
                                                                           shipmentInst)),
                                   new SboostPredicate<ByteArrayType>(LineItem::SHIPMODE,
                                                                      bind(&ByteArrayDictMultiEq::build,
                                                                           [](const ByteArray &val) {
                                                                               return val == shipMode1 ||
                                                                                      val == shipMode2;
                                                                           }))}), {lineitem});

            auto lineitemQtyFilter1 = graph.add(new ColFilter(new SboostPredicate<Int32Type>(
                    LineItem::QUANTITY, bind(&Int32DictBetween::build, 1, 11))), {lineitem});

            auto lineitemQtyFilter2 = graph.add(new ColFilter(new SboostPredicate<Int32Type>(
                    LineItem::QUANTITY, bind(&Int32DictBetween::build, 10, 20))),
                                                {lineitem});

            auto lineitemQtyFilter3 = graph.add(new ColFilter(new SboostPredicate<Int32Type>(
                    LineItem::QUANTITY, bind(&Int32DictBetween::build, 20, 30))), {lineitem});

            auto lineitemBaseMat = graph.add(new FilterMat(), {lineitemBaseFilter});

            auto validLineitem1 = graph.add(new FilterAnd(2), {lineitemQtyFilter1, lineitemBaseMat});
            auto validLineitem2 = graph.add(new FilterAnd(2), {lineitemQtyFilter2, lineitemBaseMat});
            auto validLineitem3 = graph.add(new FilterAnd(2), {lineitemQtyFilter3, lineitemBaseMat});

            auto partFilter1 = graph.add(new ColFilter(
                    {new SboostPredicate<ByteArrayType>(Part::BRAND, bind(&ByteArrayDictEq::build, brand1)),
                     new SboostPredicate<Int32Type>(Part::SIZE, bind(&Int32DictBetween::build, 1, 5)),
                     new SboostPredicate<ByteArrayType>(Part::CONTAINER, bind(&ByteArrayDictMultiEq::build,
                                                                              [](const ByteArray &val) {
                                                                                  return val == ByteArray("SM CASE") ||
                                                                                         val == ByteArray("SM BOX") ||
                                                                                         val == ByteArray("SM PACK") ||
                                                                                         val == ByteArray("SM PKG");
                                                                              }))}), {part});

            auto partFilter2 = graph.add(new ColFilter(
                    {new SboostPredicate<ByteArrayType>(Part::BRAND, bind(&ByteArrayDictEq::build, brand2)),
                     new SboostPredicate<Int32Type>(Part::SIZE, bind(&Int32DictBetween::build, 1, 10)),
                     new SboostPredicate<ByteArrayType>(Part::CONTAINER, bind(&ByteArrayDictMultiEq::build,
                                                                              [](const ByteArray &val) {
                                                                                  return val == ByteArray("MED BAG") ||
                                                                                         val == ByteArray("MED BOX") ||
                                                                                         val == ByteArray("MED PACK") ||
                                                                                         val == ByteArray("MED PKG");
                                                                              }))}), {part});

            auto partFilter3 = graph.add(new ColFilter(
                    {new SboostPredicate<ByteArrayType>(Part::BRAND, bind(&ByteArrayDictEq::build, brand3)),
                     new SboostPredicate<Int32Type>(Part::SIZE, bind(&Int32DictBetween::build, 1, 15)),
                     new SboostPredicate<ByteArrayType>(Part::CONTAINER, bind(&ByteArrayDictMultiEq::build,
                                                                              [](const ByteArray &val) {
                                                                                  return val == ByteArray("LG CASE") ||
                                                                                         val == ByteArray("LG BOX") ||
                                                                                         val == ByteArray("LG PACK") ||
                                                                                         val == ByteArray("LG PKG");
                                                                              }))}), {part});

            auto itemOnPart1 = graph.add(new FilterJoin(LineItem::PARTKEY, Part::PARTKEY),
                                         {validLineitem1, partFilter1});
            auto itemOnPart2 = graph.add(new FilterJoin(LineItem::PARTKEY, Part::PARTKEY),
                                         {validLineitem2, partFilter2});
            auto itemOnPart3 = graph.add(new FilterJoin(LineItem::PARTKEY, Part::PARTKEY),
                                         {validLineitem3, partFilter3});

            auto funion = graph.add(new FilterUnion(3), {itemOnPart1, itemOnPart2, itemOnPart3});

            auto sumagg = graph.add(new SimpleAgg([]() { return vector<AggField *>{new PriceField()}; }), {funion});

            graph.add(new Printer(PBEGIN PD(0) PEND), {sumagg});

            graph.execute();
        }

        void executeQ19() {
            auto lineitem = ParquetTable::Open(LineItem::path, {LineItem::PARTKEY, LineItem::QUANTITY,
                                                                LineItem::SHIPMODE, LineItem::SHIPINSTRUCT,
                                                                LineItem::EXTENDEDPRICE, LineItem::DISCOUNT});
            auto part = ParquetTable::Open(Part::path, {Part::PARTKEY, Part::BRAND, Part::CONTAINER, Part::SIZE});

            ColFilter lineitemBaseFilter({new SboostPredicate<ByteArrayType>(LineItem::SHIPINSTRUCT,
                                                                             bind(&ByteArrayDictEq::build,
                                                                                  shipmentInst)),
                                          new SboostPredicate<ByteArrayType>(LineItem::SHIPMODE,
                                                                             bind(&ByteArrayDictMultiEq::build,
                                                                                  [](const ByteArray &val) {
                                                                                      return val == shipMode1 ||
                                                                                             val == shipMode2;
                                                                                  }))});

            ColFilter lineitemFilter1({new SboostPredicate<Int32Type>(LineItem::QUANTITY,
                                                                      bind(&Int32DictBetween::build, 1,
                                                                           11))});
            auto qtyLineitem1 = lineitemFilter1.filter(*lineitem);

            ColFilter lineitemFilter2({new SboostPredicate<Int32Type>(LineItem::QUANTITY,
                                                                      bind(&Int32DictBetween::build, 10, 20))});
            auto qtyLineitem2 = lineitemFilter2.filter(*lineitem);

            ColFilter lineitemFilter3({new SboostPredicate<Int32Type>(LineItem::QUANTITY,
                                                                      bind(&Int32DictBetween::build, 20, 30))});
            auto qtyLineitem3 = lineitemFilter3.filter(*lineitem);

            FilterMat fmat;
            auto lineitemBase = fmat.mat(*lineitemBaseFilter.filter(*lineitem));

            FilterAnd fand(2);
            auto validLineitem1 = fand.execute(vector<Table *>{qtyLineitem1.get(), lineitemBase.get()});
            auto validLineitem2 = fand.execute(vector<Table *>{qtyLineitem2.get(), lineitemBase.get()});
            auto validLineitem3 = fand.execute(vector<Table *>{qtyLineitem3.get(), lineitemBase.get()});

            ColFilter partFilter1(
                    {new SboostPredicate<ByteArrayType>(Part::BRAND, bind(&ByteArrayDictEq::build, brand1)),
                     new SboostPredicate<Int32Type>(Part::SIZE, bind(&Int32DictBetween::build, 1, 5)),
                     new SboostPredicate<ByteArrayType>(Part::CONTAINER, bind(&ByteArrayDictMultiEq::build,
                                                                              [](const ByteArray &val) {
                                                                                  return val == ByteArray("SM CASE") ||
                                                                                         val == ByteArray("SM BOX") ||
                                                                                         val == ByteArray("SM PACK") ||
                                                                                         val == ByteArray("SM PKG");
                                                                              }))});
            auto validPart1 = partFilter1.filter(*part);

            ColFilter partFilter2(
                    {new SboostPredicate<ByteArrayType>(Part::BRAND, bind(&ByteArrayDictEq::build, brand2)),
                     new SboostPredicate<Int32Type>(Part::SIZE, bind(&Int32DictBetween::build, 1, 10)),
                     new SboostPredicate<ByteArrayType>(Part::CONTAINER, bind(&ByteArrayDictMultiEq::build,
                                                                              [](const ByteArray &val) {
                                                                                  return val == ByteArray("MED BAG") ||
                                                                                         val == ByteArray("MED BOX") ||
                                                                                         val == ByteArray("MED PACK") ||
                                                                                         val == ByteArray("MED PKG");
                                                                              }))});
            auto validPart2 = partFilter2.filter(*part);

            ColFilter partFilter3(
                    {new SboostPredicate<ByteArrayType>(Part::BRAND, bind(&ByteArrayDictEq::build, brand3)),
                     new SboostPredicate<Int32Type>(Part::SIZE, bind(&Int32DictBetween::build, 1, 15)),
                     new SboostPredicate<ByteArrayType>(Part::CONTAINER, bind(&ByteArrayDictMultiEq::build,
                                                                              [](const ByteArray &val) {
                                                                                  return val == ByteArray("LG CASE") ||
                                                                                         val == ByteArray("LG BOX") ||
                                                                                         val == ByteArray("LG PACK") ||
                                                                                         val == ByteArray("LG PKG");
                                                                              }))});
            auto validPart3 = partFilter3.filter(*part);

            FilterJoin itemOnPartJoin1(LineItem::PARTKEY, Part::PARTKEY);
            auto itemWithPart1 = itemOnPartJoin1.join(*validLineitem1, *validPart1);

            FilterJoin itemOnPartJoin2(LineItem::PARTKEY, Part::PARTKEY);
            auto itemWithPart2 = itemOnPartJoin2.join(*validLineitem2, *validPart2);

            FilterJoin itemOnPartJoin3(LineItem::PARTKEY, Part::PARTKEY);
            auto itemWithPart3 = itemOnPartJoin3.join(*validLineitem3, *validPart3);

            FilterUnion funion(3);
            auto unioneditem = funion.execute(
                    vector<Table *>{itemWithPart1.get(), itemWithPart2.get(), itemWithPart3.get()});


            SimpleAgg sumagg([]() { return vector<AggField *>{new PriceField()}; });
            auto result = sumagg.agg(*unioneditem);

            Printer printer(PBEGIN PD(0) PEND);
            printer.print(*result);
        }
    }
}