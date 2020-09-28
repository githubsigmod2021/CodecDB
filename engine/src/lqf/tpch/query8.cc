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

        using namespace agg;
        using namespace sboost;
        namespace q8 {

            ByteArray regionName("AMERICA");
            ByteArray dateFrom("1995-01-01");
            ByteArray dateTo("1996-12-31");
            ByteArray partType("ECONOMY ANODIZED STEEL");
            ByteArray nationName("GERMANY");

            class OrderJoinBuilder : public RowBuilder {
            public:
                OrderJoinBuilder() : RowBuilder(
                        {JL(LineItem::EXTENDEDPRICE), JL(LineItem::SUPPKEY), JL(LineItem::DISCOUNT),
                         JRS(Orders::ORDERDATE)}, false, true) {}

                void build(DataRow &target, DataRow &left, DataRow &right, int key) override {
                    target[0] = udf::date2year(right[0].asByteArray());
                    target[1] = left[LineItem::EXTENDEDPRICE].asDouble() * (1 - left[LineItem::DISCOUNT].asDouble());
                    target[2] = left[LineItem::SUPPKEY].asInt();
                }
            };

            class NationFilterField : public DoubleSum {
            protected:
                int nationKey_;
            public:
                NationFilterField(int nationKey) : DoubleSum(1), nationKey_(nationKey) {}

                void reduce(DataRow &input) override {
                    value_ = value_.asDouble() + ((input[2].asInt() == nationKey_) ? input[1].asDouble() : 0);
                }
            };
        }

        using namespace q8;

        void executeQ8() {
            ExecutionGraph graph;

            auto region = graph.add(new TableNode(ParquetTable::Open(Region::path, {Region::NAME, Region::REGIONKEY})),
                                    {});
            auto nationTable = ParquetTable::Open(Nation::path, {Nation::REGIONKEY, Nation::NAME, Nation::NATIONKEY});
            auto nation = graph.add(new TableNode(nationTable), {});
            auto customer = graph.add(
                    new TableNode(ParquetTable::Open(Customer::path, {Customer::NATIONKEY, Customer::CUSTKEY})), {});
            auto order = graph.add(new TableNode(
                    ParquetTable::Open(Orders::path, {Orders::CUSTKEY, Orders::ORDERDATE, Orders::ORDERKEY})), {});
            auto lineitem = graph.add(
                    new TableNode(ParquetTable::Open(LineItem::path,
                                                     {LineItem::ORDERKEY, LineItem::PARTKEY, LineItem::DISCOUNT,
                                                      LineItem::EXTENDEDPRICE, LineItem::SUPPKEY})),
                    {});
            auto part = graph.add(new TableNode(ParquetTable::Open(Part::path, {Part::TYPE, Part::PARTKEY})), {});
            auto supplier = graph.add(
                    new TableNode(ParquetTable::Open(Supplier::path, {Supplier::SUPPKEY, Supplier::NATIONKEY})), {});

            auto regionFilter = graph.add(new ColFilter({new SimplePredicate(Region::NAME, [=](const DataField &input) {
                return input.asByteArray() == regionName;
            })}), {region});

            auto nationFilter = graph.add(new FilterJoin(Nation::REGIONKEY, Region::REGIONKEY),
                                          {nation, regionFilter});

            auto customerFilter = graph.add(new FilterJoin(Customer::NATIONKEY, Nation::NATIONKEY),
                                            {customer, nationFilter});

            auto orderDateFilter = graph.add(new ColFilter(new SboostPredicate<ByteArrayType>(Orders::ORDERDATE,
                                                                                              bind(&ByteArrayDictBetween::build,
                                                                                                   dateFrom,
                                                                                                   dateTo))), {order});

            auto orderCustFilter = graph.add(new FilterJoin(Orders::CUSTKEY, Customer::CUSTKEY),
                                             {orderDateFilter, customerFilter});

            auto partFilter = graph.add(new ColFilter(
                    new SboostPredicate<ByteArrayType>(Part::TYPE, bind(&ByteArrayDictEq::build, partType))), {part});

            auto lineitemOnPartFilter = graph.add(new FilterJoin(LineItem::PARTKEY, Part::PARTKEY),
                                                  {lineitem, partFilter});

            auto orderJoin = graph.add(new HashJoin(LineItem::ORDERKEY, Orders::ORDERKEY, new OrderJoinBuilder()),
                                       {lineitemOnPartFilter, orderCustFilter});
            // YEAR PRICE SUPPKEY

            auto itemWithSupplierJoin = graph.add(
                    new HashColumnJoin(2, Supplier::SUPPKEY,
                                       new ColumnBuilder({JL(0), JL(1), JR(Supplier::NATIONKEY)})),
                    {orderJoin, supplier});
            // YEAR PRICE NATIONKEY

            int nationKey2 = KeyFinder(Nation::NATIONKEY, [=](DataRow &row) {
                return row[Nation::NAME].asByteArray() == nationName;
            }).find(*nationTable);

            auto agg = graph.add(new HashAgg(COL_HASHER(0),
                                             RowCopyFactory().field(F_REGULAR, 0, 0)->buildSnapshot(),
                                             [=]() {
                                                 return vector<AggField *>{new agg::DoubleSum(1),
                                                                           new NationFilterField(nationKey2)};
                                             }),
                                 {itemWithSupplierJoin});

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return SILE(0);
            };

            auto sort = graph.add(new SmallSort(comparator), {agg});

            graph.add(new Printer(PBEGIN PI(0) PD(1) PD(2) PEND), {sort});

            graph.execute();
        }

        void executeQ8Backup() {
            auto region = ParquetTable::Open(Region::path, {Region::NAME, Region::REGIONKEY});
            auto nation = ParquetTable::Open(Nation::path, {Nation::REGIONKEY, Nation::NAME, Nation::NATIONKEY});
            auto customer = ParquetTable::Open(Customer::path, {Customer::NATIONKEY, Customer::CUSTKEY});
            auto order = ParquetTable::Open(Orders::path, {Orders::CUSTKEY, Orders::ORDERDATE, Orders::ORDERKEY});
            auto lineitem = ParquetTable::Open(LineItem::path,
                                               {LineItem::ORDERKEY, LineItem::PARTKEY, LineItem::DISCOUNT,
                                                LineItem::EXTENDEDPRICE, LineItem::SUPPKEY});
            auto part = ParquetTable::Open(Part::path, {Part::TYPE, Part::PARTKEY});
            auto supplier = ParquetTable::Open(Supplier::path, {Supplier::SUPPKEY, Supplier::NATIONKEY});


            ColFilter regionFilter({new SimplePredicate(Region::NAME, [=](const DataField &input) {
                return input.asByteArray() == regionName;
            })});
            auto validRegion = regionFilter.filter(*region);

            FilterJoin nationFilter(Nation::REGIONKEY, Region::REGIONKEY);
            auto validNation = nationFilter.join(*nation, *validRegion);

            FilterJoin customerFilter(Customer::NATIONKEY, Nation::NATIONKEY);
            auto validCust = customerFilter.join(*customer, *validNation);

            ColFilter orderDateFilter({new SboostPredicate<ByteArrayType>(Orders::ORDERDATE,
                                                                          bind(&ByteArrayDictBetween::build, dateFrom,
                                                                               dateTo))});
            auto validOrder = orderDateFilter.filter(*order);

            FilterJoin orderCustFilter(Orders::CUSTKEY, Customer::CUSTKEY);
            validOrder = orderCustFilter.join(*validOrder, *validCust);

            ColFilter partFilter(
                    {new SboostPredicate<ByteArrayType>(Part::TYPE, bind(&ByteArrayDictEq::build, partType))});
            auto validPart = partFilter.filter(*part);

            FilterJoin lineitemOnPartFilter(LineItem::PARTKEY, Part::PARTKEY);
            auto validLineitem = lineitemOnPartFilter.join(*lineitem, *validPart);


            HashJoin orderJoin(LineItem::ORDERKEY, Orders::ORDERKEY, new OrderJoinBuilder());
            // YEAR PRICE SUPPKEY
            auto itemWithOrder = orderJoin.join(*validLineitem, *validOrder);

            HashColumnJoin itemWithSupplierJoin(2, Supplier::SUPPKEY,
                                                new ColumnBuilder({JL(0), JL(1), JR(Supplier::NATIONKEY)}));
            // YEAR PRICE NATIONKEY
            auto result = itemWithSupplierJoin.join(*itemWithOrder, *supplier);

            int nationKey2 = KeyFinder(Nation::NATIONKEY, [=](DataRow &row) {
                return row[Nation::NAME].asByteArray() == nationName;
            }).find(*nation);
            HashAgg agg(COL_HASHER(0), RowCopyFactory().field(F_REGULAR, 0, 0)->buildSnapshot(),
                        [=]() {
                            return vector<AggField *>({new agg::DoubleSum(1), new NationFilterField(nationKey2)});
                        });
            result = agg.agg(*result);

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return SILE(0);
            };
            SmallSort sort(comparator);
            auto sorted = sort.sort(*result);

            Printer printer(PBEGIN PI(0) PD(1) PD(2) PEND);
            printer.print(*sorted);
        }

        void executeQ8Debug() {
            auto order = ParquetTable::Open(Orders::path, {Orders::CUSTKEY, Orders::ORDERDATE, Orders::ORDERKEY});

            ColFilter orderDateFilter({new SboostPredicate<ByteArrayType>(Orders::ORDERDATE,
                                                                          bind(&ByteArrayDictBetween::build, dateFrom,
                                                                               dateTo))});
            ColFilter orderDateSimpleFilter(new SimplePredicate(Orders::ORDERDATE, [](const DataField &field) {
                auto val = field.asByteArray();
                return (val < dateTo && val > dateFrom) || val == dateTo || val == dateFrom;
            }));
            auto validOrder = orderDateFilter.filter(*order);

            Printer printer(PBEGIN PI(Orders::ORDERKEY) PB(Orders::ORDERDATE) PEND);
//            printer.print(*order);
            printer.print(*validOrder);
        }
    }
}