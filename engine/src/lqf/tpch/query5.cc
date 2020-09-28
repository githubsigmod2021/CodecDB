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
        namespace q5 {
            ByteArray dateFrom("1994-01-01");
            ByteArray dateTo("1995-01-01");
            ByteArray region("ASIA");

            class ItemPriceRowBuilder : public RowBuilder {
            public:
                ItemPriceRowBuilder() : RowBuilder(
                        {JL(LineItem::ORDERKEY), JR(Supplier::NATIONKEY), JL(LineItem::EXTENDEDPRICE)}, false, true) {}

                void build(DataRow &target, DataRow &left, DataRow &right, int32_t key) override {
                    target[0] = left[LineItem::ORDERKEY];
                    target[1] = right[0];
                    target[2] = left[LineItem::EXTENDEDPRICE].asDouble() * (1 - left[LineItem::DISCOUNT].asDouble());
                }
            };
        }
        using namespace agg;
        using namespace sboost;
        using namespace powerjoin;

        void executeQ5() {
            auto regionTable = ParquetTable::Open(Region::path, {Region::REGIONKEY, Region::NAME});
            auto nationTable = ParquetTable::Open(Nation::path, {Nation::REGIONKEY, Nation::NAME, Nation::NATIONKEY});
            auto supplierTable = ParquetTable::Open(Supplier::path, {Supplier::NATIONKEY, Supplier::SUPPKEY});
            auto customerTable = ParquetTable::Open(Customer::path, {Customer::CUSTKEY, Customer::NATIONKEY});
            auto orderTable = ParquetTable::Open(Orders::path, {Orders::CUSTKEY, Orders::ORDERKEY, Orders::ORDERDATE});
            auto lineitemTable = ParquetTable::Open(LineItem::path, {LineItem::DISCOUNT, LineItem::EXTENDEDPRICE,
                                                                     LineItem::ORDERKEY, LineItem::SUPPKEY});

            ExecutionGraph graph;

            auto region = graph.add(new TableNode(regionTable), {});
            auto nation = graph.add(new TableNode(nationTable), {});
            auto supplier = graph.add(new TableNode(supplierTable), {});
            auto customer = graph.add(new TableNode(customerTable), {});
            auto order = graph.add(new TableNode(orderTable), {});
            auto lineitem = graph.add(new TableNode(lineitemTable), {});

            auto regionFilter = graph.add(new ColFilter(new SimplePredicate(Region::NAME, [](const DataField &df) {
                return df.asByteArray() == q5::region;
            })), {region});

            auto nationFilterJoin = graph.add(new FilterJoin(Nation::REGIONKEY, Region::REGIONKEY),
                                              {nation, regionFilter});

            auto matNation = graph.add(new HashMat(Nation::NATIONKEY, RowCopyFactory()
                    .from(EXTERNAL)->to(RAW)
                    ->field(F_STRING, Nation::NAME, 0)->buildSnapshot(), 50), {nationFilterJoin});

            auto custNationFilter = graph.add(new FilterJoin(Customer::NATIONKEY, Nation::NATIONKEY),
                                              {customer, matNation});

            auto suppNationFilter = graph.add(new FilterJoin(Supplier::NATIONKEY, Nation::NATIONKEY),
                                              {supplier, matNation});

            auto orderFilter = graph.add(new ColFilter(new SboostPredicate<ByteArrayType>(Orders::ORDERDATE,
                                                                                          bind(&ByteArrayDictRangele::build,
                                                                                               q5::dateFrom,
                                                                                               q5::dateTo))), {order});

            auto orderOnCustomerJoin = graph.add(new HashJoin(Orders::CUSTKEY, Customer::CUSTKEY,
                                                              new RowBuilder(
                                                                      {JL(Orders::ORDERKEY), JR(Customer::NATIONKEY)},
                                                                      false, true)), {orderFilter, custNationFilter});
            // ORDERKEY, NATIONKEY

            auto itemOnSupplierJoin = graph.add(
                    new HashJoin(LineItem::SUPPKEY, Supplier::SUPPKEY, new q5::ItemPriceRowBuilder(), nullptr, 45000),
                    {lineitem, suppNationFilter});
            // ORDERKEY NATIONKEY PRICE

            function<uint64_t(DataRow &)> key_maker = [](DataRow &dr) {
                return (static_cast<uint64_t>(dr[0].asInt()) << 32) + dr[1].asInt();
            };
            auto orderItemJoin = graph.add(new PowerHashFilterJoin(key_maker, key_maker),
                                           {itemOnSupplierJoin, orderOnCustomerJoin});
            // ORDERKEY NATIONKEY PRICE

            auto pagg = new HashAgg(COL_HASHER(1),
                                    RowCopyFactory().field(F_REGULAR, 1, 0)->buildSnapshot(),
                                    []() { return vector<AggField *>{new agg::DoubleSum(2)}; }, nullptr, true);

            auto agg = graph.add(pagg, {orderItemJoin});
            // NATIONKEY PRICE

            auto addNationNameJoin = graph.add(new HashColumnJoin(0, 0, new ColumnBuilder({JL(1), JRS(0)}), 10),
                                               {agg, matNation});
            // PRICE NATIONNAME

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return (*a)[1].asByteArray() < (*b)[1].asByteArray();
            };
            auto sort = graph.add(new SmallSort(comparator), {addNationNameJoin});

            graph.add(new Printer(PBEGIN PD(0) PB(1) PEND), {sort});

            graph.execute();
        }

        using namespace q5;

        void executeQ5Backup() {
            auto regionTable = ParquetTable::Open(Region::path, {Region::REGIONKEY, Region::NAME});
            auto nationTable = ParquetTable::Open(Nation::path, {Nation::REGIONKEY, Nation::NAME, Nation::NATIONKEY});
            auto supplierTable = ParquetTable::Open(Supplier::path, {Supplier::NATIONKEY, Supplier::SUPPKEY});
            auto customerTable = ParquetTable::Open(Customer::path, {Customer::CUSTKEY, Customer::NATIONKEY});
            auto orderTable = ParquetTable::Open(Orders::path, {Orders::CUSTKEY, Orders::ORDERKEY, Orders::ORDERDATE});
            auto lineitemTable = ParquetTable::Open(LineItem::path, {LineItem::DISCOUNT, LineItem::EXTENDEDPRICE,
                                                                     LineItem::ORDERKEY, LineItem::SUPPKEY});

            ColFilter regionFilter({new SimplePredicate(Region::NAME, [](const DataField &df) {
                return df.asByteArray() == region;
            })});
            auto validRegion = regionFilter.filter(*regionTable);

            FilterJoin nationFilterJoin(Nation::REGIONKEY, Region::REGIONKEY);
            auto validNation = nationFilterJoin.join(*nationTable, *validRegion);

            auto mat = HashMat(Nation::NATIONKEY, RowCopyFactory()
                    .from(EXTERNAL)->to(RAW)
                    ->field(F_STRING, Nation::NAME, 0)->buildSnapshot(), 50);
            auto matNation = mat.mat(*validNation);

            FilterJoin custNationFilter(Customer::NATIONKEY, Nation::NATIONKEY);
            auto validCustomer = custNationFilter.join(*customerTable, *matNation);

            FilterJoin suppNationFilter(Supplier::NATIONKEY, Nation::NATIONKEY);
            auto validSupplier = suppNationFilter.join(*supplierTable, *matNation);

            ColFilter orderFilter({new SboostPredicate<ByteArrayType>(Orders::ORDERDATE,
                                                                      bind(&ByteArrayDictRangele::build, dateFrom,
                                                                           dateTo))});
            auto validOrder = orderFilter.filter(*orderTable);

//            cout << validCustomer->size() << endl;
            HashJoin orderOnCustomerJoin(Orders::CUSTKEY, Customer::CUSTKEY,
                                         new RowBuilder({JL(Orders::ORDERKEY), JR(Customer::NATIONKEY)}, false, true));
            // ORDERKEY, NATIONKEY
            validOrder = orderOnCustomerJoin.join(*validOrder, *validCustomer);

            HashJoin itemOnSupplierJoin(LineItem::SUPPKEY, Supplier::SUPPKEY, new ItemPriceRowBuilder(), nullptr,
                                        45000);
            // ORDERKEY NATIONKEY PRICE
            auto validLineitem = itemOnSupplierJoin.join(*lineitemTable, *validSupplier);

//            cout << validLineitem->size() << endl;
            function<uint64_t(DataRow &)> key_maker = [](DataRow &dr) {
                return (static_cast<uint64_t>(dr[0].asInt()) << 32) + dr[1].asInt();
            };
            PowerHashFilterJoin orderItemJoin(key_maker, key_maker);
            // ORDERKEY NATIONKEY PRICE
            auto joined = orderItemJoin.join(*validLineitem, *validOrder);

            HashAgg agg(COL_HASHER(1), RowCopyFactory().field(F_REGULAR, 1, 0)->buildSnapshot(),
                        []() { return vector<AggField *>{new agg::DoubleSum(2)}; }, nullptr, true);
            // NATIONKEY PRICE
            auto agged = agg.agg(*joined);

            HashColumnJoin addNationNameJoin(0, 0, new ColumnBuilder({JL(1), JRS(0)}));
            // PRICE NATIONNAME
            auto result = addNationNameJoin.join(*agged, *matNation);

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return (*a)[1].asByteArray() < (*b)[1].asByteArray();
            };
            SmallSort sort(comparator);
            auto sorted = sort.sort(*result);

            Printer printer(PBEGIN PD(0) PB(1) PEND);
            printer.print(*sorted);
        }
    }
}