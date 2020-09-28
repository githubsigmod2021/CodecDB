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
        namespace q7 {
            string nation1("FRANCE");
            string nation2("GERMANY");

            ByteArray dateFrom("1995-01-01");
            ByteArray dateTo("1996-12-31");

            class Q7ItemWithNationBuilder : public RowBuilder {
            public:
                Q7ItemWithNationBuilder() : RowBuilder(
                        {JL(LineItem::ORDERKEY), JR(Supplier::NATIONKEY), JL(0), JL(LineItem::EXTENDEDPRICE)}, false,
                        true) {}

                void build(DataRow &target, DataRow &left, DataRow &right, int key) {
                    target[0] = left[LineItem::ORDERKEY];
                    target[1] = right[0];
                    target[2] = udf::date2year(left[LineItem::SHIPDATE].asByteArray());
                    target[3] = left[LineItem::EXTENDEDPRICE].asDouble() * (1 - left[LineItem::DISCOUNT].asDouble());
                }
            };
        }

        using namespace q7;

        void executeQ7() {
            auto nationTable = ParquetTable::Open(Nation::path, {Nation::NAME, Nation::NATIONKEY});
            HashMat nationMat(Nation::NATIONKEY, RowCopyFactory()
                    .from(EXTERNAL)->to(RAW)
                    ->field(F_STRING, Nation::NAME, 0)->buildSnapshot());
            auto matNation = nationMat.mat(*nationTable);

            unordered_map<string, int32_t> nation_mapping;
            nationTable->blocks()->foreach([&nation_mapping](const shared_ptr<Block> &block) {
                auto rows = block->rows();
                auto block_size = block->size();
                for (uint32_t i = 0; i < block_size; ++i) {
                    DataRow &row = rows->next();
                    stringstream ks;
                    ks << row[Nation::NAME].asByteArray();
                    nation_mapping[ks.str()] = row[Nation::NATIONKEY].asInt();
                }
            });

            int nationKey1 = nation_mapping[nation1];
            int nationKey2 = nation_mapping[nation2];

            auto customerTable = ParquetTable::Open(Customer::path, {Customer::NATIONKEY, Customer::CUSTKEY});
            auto supplierTable = ParquetTable::Open(Supplier::path, {Supplier::SUPPKEY, Supplier::NATIONKEY});
            auto orderTable = ParquetTable::Open(Orders::path, {Orders::CUSTKEY, Orders::ORDERKEY});
            auto lineitemTable = ParquetTable::Open(LineItem::path,
                                                    {LineItem::SUPPKEY, LineItem::ORDERKEY, LineItem::SHIPDATE,
                                                     LineItem::EXTENDEDPRICE, LineItem::DISCOUNT});

            ExecutionGraph graph;

            auto customer = graph.add(new TableNode(customerTable), {});
            auto supplier = graph.add(new TableNode(supplierTable), {});
            auto order = graph.add(new TableNode(orderTable), {});
            auto lineitem = graph.add(new TableNode(lineitemTable), {});
            auto nation = graph.add(new TableNode(matNation), {});

            function<bool(int32_t)> nation_key_pred = [=](int32_t key) {
                return key == nationKey1 || key == nationKey2;
            };

            auto validCustomerFilter = graph.add(
                    new ColFilter(new SboostPredicate<Int32Type>(Customer::NATIONKEY,
                                                                 bind(&Int32DictMultiEq::build, nation_key_pred))),
                    {customer});

            auto validSupplierFilter = graph.add(new ColFilter(new SboostPredicate<Int32Type>(Customer::NATIONKEY,
                                                                                              bind(&Int32DictMultiEq::build,
                                                                                                   nation_key_pred))),
                                                 {supplier});

            auto orderWithNationJoin = graph.add(
                    new HashJoin(Orders::CUSTKEY, Customer::CUSTKEY,
                                 new RowBuilder({JL(Orders::ORDERKEY), JR(Customer::NATIONKEY)})),
                    {order, validCustomerFilter});
            // ORDERKEY, NATIONKEY

            auto lineitemFilter = graph.add(
                    new ColFilter(new SboostPredicate<ByteArrayType>(LineItem::SHIPDATE,
                                                                     bind(&ByteArrayDictBetween::build, dateFrom,
                                                                          dateTo))),
                    {lineitem});

            auto itemWithNationJoin = graph.add(
                    new HashJoin(LineItem::SUPPKEY, Supplier::SUPPKEY, new Q7ItemWithNationBuilder()),
                    {lineitemFilter, validSupplierFilter});
            // ORDERKEY, NATIONKEY, YEAR, PRICE

            auto itemWithOrderJoin = graph.add(
                    new HashJoin(0, 0, new RowBuilder({JL(1), JR(1), JL(2), JL(3)}),
                                 [](DataRow &left, DataRow &right) {
                                     return left[1].asInt() != right[0].asInt();
                                 }, 2500000), {itemWithNationJoin, orderWithNationJoin});
            // Customer::NATIONKEY, Supplier::NATIONKEY, YEAR, PRICE

            function<uint64_t(DataRow &)> indexer = [](DataRow &input) {
                return (input[0].asInt() << 16) + (input[1].asInt() << 11) + input[2].asInt();
            };
            auto pagg = new HashAgg(indexer,
                                    RowCopyFactory().field(F_REGULAR, 0, 0)
                                            ->field(F_REGULAR, 1, 1)
                                            ->field(F_REGULAR, 2, 2)->buildSnapshot(),
                                    []() { return vector<AggField *>({new DoubleSum(3)}); }, nullptr, true);
            auto agg = graph.add(pagg, {itemWithOrderJoin});

            auto joinNation1 = graph.add(new HashColumnJoin(0, 0, new ColumnBuilder({JRS(0), JL(1), JL(2), JL(3)})),
                                         {agg, nation});
            auto joinNation2 = graph.add(new HashColumnJoin(1, 0, new ColumnBuilder({JLS(0), JRS(0), JL(2), JL(3)})),
                                         {joinNation1, nation});

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return SBLE(0) || (SBE(0) && SBLE(1)) || (SBE(0) && SBE(1) && SILE(2));
            };
            auto sort = graph.add(new SmallSort(comparator), {joinNation2});

            graph.add(new Printer(PBEGIN PB(0) PB(1) PI(2) PD(3) PEND), {sort});

            graph.execute();
        }

        void executeQ7Backup() {
            auto nationTable = ParquetTable::Open(Nation::path, {Nation::NAME, Nation::NATIONKEY});
            HashMat nationMat(Nation::NATIONKEY, RowCopyFactory()
                    .from(EXTERNAL)->to(RAW)
                    ->field(F_STRING, Nation::NAME, 0)->buildSnapshot());
            auto matNation = nationMat.mat(*nationTable);

            unordered_map<string, int32_t> nation_mapping;
            nationTable->blocks()->foreach([&nation_mapping](const shared_ptr<Block> &block) {
                auto rows = block->rows();
                auto block_size = block->size();
                for (uint32_t i = 0; i < block_size; ++i) {
                    DataRow &row = rows->next();
                    stringstream ks;
                    ks << row[Nation::NAME].asByteArray();
                    nation_mapping[ks.str()] = row[Nation::NATIONKEY].asInt();
                }
            });

            int nationKey1 = nation_mapping[nation1];
            int nationKey2 = nation_mapping[nation2];

            auto customerTable = ParquetTable::Open(Customer::path, {Customer::NATIONKEY, Customer::CUSTKEY});
            auto supplierTable = ParquetTable::Open(Supplier::path, {Supplier::SUPPKEY, Supplier::NATIONKEY});
            auto orderTable = ParquetTable::Open(Orders::path, {Orders::CUSTKEY, Orders::ORDERKEY});
            auto lineitemTable = ParquetTable::Open(LineItem::path,
                                                    {LineItem::SUPPKEY, LineItem::ORDERKEY, LineItem::SHIPDATE,
                                                     LineItem::EXTENDEDPRICE, LineItem::DISCOUNT});

            function<bool(int32_t)> nation_key_pred = [=](int32_t key) {
                return key == nationKey1 || key == nationKey2;
            };
            ColFilter validCustomerFilter({new SboostPredicate<Int32Type>(Customer::NATIONKEY,
                                                                          bind(&Int32DictMultiEq::build,
                                                                               nation_key_pred))});
            auto validCustomer = validCustomerFilter.filter(*customerTable);

            ColFilter validSupplierFilter({new SboostPredicate<Int32Type>(Customer::NATIONKEY,
                                                                          bind(&Int32DictMultiEq::build,
                                                                               nation_key_pred))});
            auto validSupplier = validSupplierFilter.filter(*supplierTable);

            HashJoin orderWithNationJoin(Orders::CUSTKEY, Customer::CUSTKEY,
                                         new RowBuilder({JL(Orders::ORDERKEY), JR(Customer::NATIONKEY)}));
            // ORDERKEY, NATIONKEY
            auto orderWithNation = orderWithNationJoin.join(*orderTable, *validCustomer);

            ColFilter lineitemFilter({new SboostPredicate<ByteArrayType>(LineItem::SHIPDATE,
                                                                         bind(&ByteArrayDictBetween::build, dateFrom,
                                                                              dateTo))});
            auto filteredLineitem = lineitemFilter.filter(*lineitemTable);

            HashJoin itemWithNationJoin(LineItem::SUPPKEY, Supplier::SUPPKEY, new Q7ItemWithNationBuilder(), nullptr,
                                        true);
            // ORDERKEY, NATIONKEY, YEAR, PRICE
            auto itemWithNation = itemWithNationJoin.join(*filteredLineitem, *validSupplier);

            HashJoin itemWithOrderJoin(0, 0, new RowBuilder({JL(1), JR(1), JL(2), JL(3)}),
                                       [](DataRow &left, DataRow &right) {
                                           return left[1].asInt() != right[0].asInt();
                                       }, 2500000);
//            cout << orderWithNation->size() << endl;
            // Customer::NATIONKEY, Supplier::NATIONKEY, YEAR, PRICE
            auto joined = itemWithOrderJoin.join(*itemWithNation, *orderWithNation);

            function<uint64_t(DataRow &)> indexer = [](DataRow &input) {
                return (input[0].asInt() << 16) + (input[1].asInt() << 11) + input[2].asInt();
            };
            HashAgg agg(indexer, RowCopyFactory().field(F_REGULAR, 0, 0)->field(F_REGULAR, 1, 1)
                                ->field(F_REGULAR, 2, 2)->buildSnapshot(),
                        []() { return vector<AggField *>({new DoubleSum(3)}); });
            auto agged = agg.agg(*joined);

            HashColumnJoin joinNation1(0, 0, new ColumnBuilder({JRS(0), JL(1), JL(2), JL(3)}));
            agged = joinNation1.join(*agged, *matNation);

            HashColumnJoin joinNation2(1, 0, new ColumnBuilder({JLS(0), JRS(0), JL(2), JL(3)}));
            agged = joinNation2.join(*agged, *matNation);

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return SBLE(0) || (SBE(0) && SBLE(1)) || (SBE(0) && SBE(1) && SILE(2));
            };
            SmallSort sort(comparator);
            auto sorted = sort.sort(*agged);

            Printer printer(PBEGIN PB(0) PB(1) PI(2) PD(3) PEND);
            printer.print(*sorted);
        }
    }
}