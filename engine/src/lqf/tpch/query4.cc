//
// Created by harper on 4/2/20.
//

#include <unordered_set>
#include <lqf/filter.h>
#include <lqf/data_model.h>
#include <parquet/types.h>
#include <lqf/mat.h>
#include <lqf/join.h>
#include <lqf/agg.h>
#include <lqf/sort.h>
#include <lqf/print.h>
#include "tpchquery.h"

namespace lqf {
    namespace tpch {
        namespace q4 {
            ByteArray dateFrom("1993-07-01");
            ByteArray dateTo("1993-10-01");
        }
        using namespace sboost;
        using namespace q4;

        void executeQ4() {
            ExecutionGraph graph;

            auto orderTable = ParquetTable::Open(Orders::path,
                                                 {Orders::ORDERDATE, Orders::ORDERKEY, Orders::ORDERPRIORITY});
            auto lineitemTable = ParquetTable::Open(LineItem::path,
                                                    {LineItem::ORDERKEY, LineItem::RECEIPTDATE, LineItem::COMMITDATE});

            auto order = graph.add(new TableNode(orderTable), {});
            auto lineitem = graph.add(new TableNode(lineitemTable), {});

            auto orderFilter = graph.add(new ColFilter(
                    new SboostPredicate<ByteArrayType>(Orders::ORDERDATE,
                                                       bind(&ByteArrayDictRangele::build, dateFrom, dateTo))),
                                         {order});

//            auto lineItemFilter = graph.add(new RowFilter([](DataRow &datarow) {
//                return datarow[LineItem::COMMITDATE].asByteArray() < datarow[LineItem::RECEIPTDATE].asByteArray();
//            }), {lineitem});
            auto lineItemFilter = graph.add(new SboostRowFilter(LineItem::COMMITDATE, LineItem::RECEIPTDATE),
                                            {lineitem});

            auto existJoin = graph.add(new FilterJoin(Orders::ORDERKEY, LineItem::ORDERKEY, 3000000),
                                       {orderFilter, lineItemFilter});

            function<uint64_t(DataRow &)> indexer = [](DataRow &row) {
                return row(Orders::ORDERPRIORITY).asInt();
            };

            auto agg = graph.add(new HashAgg(indexer,
                                             RowCopyFactory().field(F_RAW, Orders::ORDERPRIORITY, 0)->buildSnapshot(),
                                             []() { return vector<agg::AggField *>{new agg::Count()}; }), {existJoin});

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return (*a)[0].asInt() < (*b)[0].asInt();
            };
            auto sort = graph.add(new SmallSort(comparator), {agg});

            auto opdict = orderTable->LoadDictionary<ByteArrayType>(Orders::ORDERPRIORITY);
            auto opdictp = opdict.get();

            graph.add(new Printer(PBEGIN PDICT(opdictp, 0) PI(1) PEND), {sort});

            graph.execute();
        }

        void executeQ4Backup() {

            auto orderTable = ParquetTable::Open(Orders::path,
                                                 {Orders::ORDERDATE, Orders::ORDERKEY, Orders::ORDERPRIORITY});
            auto lineitemTable = ParquetTable::Open(LineItem::path,
                                                    {LineItem::ORDERKEY, LineItem::RECEIPTDATE, LineItem::COMMITDATE});

            ColFilter orderFilter(
                    {new SboostPredicate<ByteArrayType>(Orders::ORDERDATE,
                                                        bind(&ByteArrayDictRangele::build, dateFrom, dateTo))});
            auto filteredOrderTable = orderFilter.filter(*orderTable);

            SboostRowFilter lineItemFilter(LineItem::COMMITDATE, LineItem::RECEIPTDATE);
            auto filteredLineItemTable = lineItemFilter.filter(*lineitemTable);

//            auto set = make_shared<unordered_set<int32_t>>();
//            auto max = 0;

//            filteredLineItemTable->blocks()->foreach([set, &max](const shared_ptr<Block> &b) {
//                auto col = b->col(LineItem::ORDERKEY);
//                auto bsize = b->size();
//                for (uint32_t i = 0; i < bsize; ++i) {
//                    auto val = col->next().asInt();
//                    max = std::max(val, max);
//                    set->insert(val);
//                }
//            });
//
//            cout << set->size() << "," << max << endl;

            FilterJoin existJoin(Orders::ORDERKEY, LineItem::ORDERKEY, 3000000);
            auto existOrderTable = existJoin.join(*filteredOrderTable, *filteredLineItemTable);
//
            function<uint64_t(DataRow &)> indexer = [](DataRow &row) {
                return row(Orders::ORDERPRIORITY).asInt();
            };

            HashAgg agg(indexer, RowCopyFactory().field(F_RAW, Orders::ORDERPRIORITY, 0)->buildSnapshot(),
                        []() { return vector<agg::AggField *>{new agg::Count()}; });

            auto agged = agg.agg(*existOrderTable);

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return (*a)[0].asInt() < (*b)[0].asInt();
            };
            SmallSort sort(comparator);
            auto sorted = sort.sort(*agged);

            auto opdict = orderTable->LoadDictionary<ByteArrayType>(Orders::ORDERPRIORITY);
            auto opdictp = opdict.get();
            Printer printer(PBEGIN PDICT(opdictp, 0) PI(1) PEND);
            printer.print(*sorted);

        }
    }
}