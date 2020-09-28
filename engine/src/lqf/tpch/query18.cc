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
        namespace q18 {

            int quantity = 300;
        }

        using namespace q18;

        void executeQ18_Graph() {
            ExecutionGraph graph;

            auto order = graph.add(new TableNode(ParquetTable::Open(Orders::path,
                                                                    {Orders::ORDERKEY, Orders::ORDERDATE,
                                                                     Orders::TOTALPRICE, Orders::CUSTKEY})), {});
            auto lineitem = graph.add(
                    new TableNode(ParquetTable::Open(LineItem::path, {LineItem::ORDERKEY, LineItem::QUANTITY})), {});
            auto customer = graph.add(
                    new TableNode(ParquetTable::Open(Customer::path, {Customer::NAME, Customer::CUSTKEY})), {});

            auto hashAgg = graph.add(
                    new StripeHashAgg(32, COL_HASHER(LineItem::ORDERKEY), COL_HASHER(0),
                                      RowCopyFactory().field(F_REGULAR, LineItem::ORDERKEY, 0)
                                              ->field(F_REGULAR, LineItem::QUANTITY, 1)->buildSnapshot(),
                                      RowCopyFactory().field(F_REGULAR, 0, 0)->buildSnapshot(),
                                      []() { return vector<AggField *>{new IntSum(1)}; },
                                      [=](DataRow &row) { return row[1].asInt() > quantity; }),
                    {lineitem});
            // ORDERKEY, SUM_QUANTITY

            auto withOrderJoin = graph.add(new HashJoin(Orders::ORDERKEY, 0, new RowBuilder(
                    {JL(Orders::CUSTKEY), JLR(Orders::ORDERDATE), JL(Orders::TOTALPRICE), JR(1)}, true)),
                                           {order, hashAgg});
            // ORDERKEY, CUSTKEY, ORDERDATE, TOTALPRICE, SUM_QUANTITY

            auto withCustomerJoin = graph.add(
                    new HashMultiJoin(Customer::CUSTKEY, 1,
                                      new RowBuilder(
                                              {JR(0), JR(1), JR(2), JR(3), JR(4), JLS(Customer::NAME)})),
                    {customer, withOrderJoin});
            // ORDERKEY, CUSTKEY, ORDERDATE, TOTALPRICE, SUM_QUANTITY,CUSTNAME

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return SDGE(3) || (SDE(3) && SBLE(2));
            };
            auto topn = graph.add(new TopN(100, comparator), {withCustomerJoin});

            graph.add(new Printer(PBEGIN PB(5) PI(1) PI(0) PI(2) PD(3) PI(4) PEND), {topn});
            graph.execute();
        }

        void executeQ18() {

            auto order = ParquetTable::Open(Orders::path,
                                            {Orders::ORDERKEY, Orders::ORDERDATE, Orders::TOTALPRICE, Orders::CUSTKEY});
            auto lineitem = ParquetTable::Open(LineItem::path, {LineItem::ORDERKEY, LineItem::QUANTITY});
            auto customer = ParquetTable::Open(Customer::path, {Customer::NAME, Customer::CUSTKEY});

            StripeHashAgg hashAgg(32, COL_HASHER(LineItem::ORDERKEY), COL_HASHER(0),
                                  RowCopyFactory().field(F_REGULAR, LineItem::ORDERKEY, 0)
                                          ->field(F_REGULAR, LineItem::QUANTITY, 1)->buildSnapshot(),
                                  RowCopyFactory().field(F_REGULAR, 0, 0)->buildSnapshot(),
                                  []() { return vector<AggField *>{new IntSum(1)}; },
                                  [=](DataRow &row) { return row[1].asInt() > quantity; });
            // ORDERKEY, SUM_QUANTITY
            auto validLineitem = hashAgg.agg(*lineitem);

            HashJoin withOrderJoin(Orders::ORDERKEY, 0,
                                   new RowBuilder(
                                           {JL(Orders::CUSTKEY), JLR(Orders::ORDERDATE), JL(Orders::TOTALPRICE), JR(1)},
                                           true));
            // ORDERKEY, CUSTKEY, ORDERDATE, TOTALPRICE, SUM_QUANTITY
            auto withOrder = withOrderJoin.join(*order, *validLineitem);

            HashMultiJoin withCustomerJoin(Customer::CUSTKEY, 1,
                                           new RowBuilder(
                                                   {JR(0), JR(1), JR(2), JR(3), JR(4), JLS(Customer::NAME)}));
            // ORDERKEY, CUSTKEY, ORDERDATE, TOTALPRICE, SUM_QUANTITY,CUSTNAME
            auto withCustomer = withCustomerJoin.join(*customer, *withOrder);

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return SDGE(3) || (SDE(3) && SBLE(2));
            };
            TopN topn(100, comparator);
            auto sorted = topn.sort(*withCustomer);

            auto dict = order->LoadDictionary<ByteArrayType>(Orders::ORDERDATE);
            auto pdict = dict.get();
            Printer printer(PBEGIN PB(5) PI(1) PI(0) PDICT(pdict, 2) PD(3) PI(4) PEND);
            printer.print(*sorted);
        }
    }
}