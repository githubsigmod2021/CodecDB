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
#include <lqf/util.h>
#include "tpchquery.h"


namespace lqf {
    namespace tpch {
        namespace q9 {
            class ItemWithOrderBuilder : public RowBuilder {

            public:
                ItemWithOrderBuilder() : RowBuilder({JL(LineItem::PARTKEY), JL(LineItem::SUPPKEY),
                                                     JL(LineItem::EXTENDEDPRICE), JL(LineItem::DISCOUNT),
                                                     JL(LineItem::QUANTITY), JRS(Orders::ORDERDATE)}, false, true) {}


                void build(DataRow &target, DataRow &left, DataRow &right, int key) {
                    target[0] = left[LineItem::PARTKEY].asInt();
                    target[1] = left[LineItem::SUPPKEY].asInt();
                    target[2] = left[LineItem::EXTENDEDPRICE].asDouble() * (1 - left[LineItem::DISCOUNT].asDouble());
                    target[3] = left[LineItem::QUANTITY].asInt();
                    target[4] = udf::date2year(right[0].asByteArray());
                }
            };

            class ItemWithRevBuilder : public RowBuilder {
            public :
                ItemWithRevBuilder() : RowBuilder({JL(2), JL(3), JR(PartSupp::SUPPLYCOST)},
                                                  false, true) {}

                void build(DataRow &target, DataRow &left, DataRow &right, int key) {
                    target[0] = left[1].asInt(); // SUPPKEY
                    target[1] = left[4].asInt(); // YEAR
                    target[2] = left[2].asDouble() - left[3].asInt() * right[0].asDouble(); // REV
                }
            };

            class ItemProcessorOutput : public NodeOutput {
            public:
                HashPredicate<Int32> orderkeys_;
                HashPredicate<Int32> suppkeys_;
                HashPredicate<Int64> partsuppkeys_;
            };

            class ItemProcessor : public Node {
            public:
                ItemProcessor() : Node(1) {}

                unique_ptr<NodeOutput> execute(const vector<NodeOutput *> &input) override {
                    auto lineitem = static_cast<TableOutput *>(input[0])->get();
                    auto output = new ItemProcessorOutput();
                    lineitem->blocks()->foreach([output](const shared_ptr<Block> &block) {
                        auto rows = block->rows();
                        auto block_size = block->size();
                        for (uint32_t i = 0; i < block_size; ++i) {
                            DataRow &row = rows->next();
                            output->orderkeys_.add(row[LineItem::ORDERKEY].asInt());
                            int suppkey = row[LineItem::SUPPKEY].asInt();
                            int partkey = row[LineItem::PARTKEY].asInt();
                            output->suppkeys_.add(suppkey);
                            output->partsuppkeys_.add((static_cast<uint64_t>(partkey) << 32) + suppkey);
                        }
                    });
                    return unique_ptr<NodeOutput>(output);
                }
            };

            class OrderFilter : public NestedNode {
            public:
                OrderFilter(MapFilter *inner) : NestedNode(inner, 2, true) {}

                unique_ptr<NodeOutput> execute(const vector<NodeOutput *> &input) override {
                    auto filter = dynamic_cast<MapFilter *>(inner_.get());
                    auto table = static_cast<TableOutput *>(input[0])->get();
                    auto map = static_cast<ItemProcessorOutput *>(input[1]);
                    filter->setMap(map->orderkeys_);
                    auto output = filter->filter(*table);
                    return unique_ptr<NodeOutput>(new TableOutput(output));
                }
            };

            class SupplierFilter : public NestedNode {
            public:
                SupplierFilter(MapFilter *inner) : NestedNode(inner, 2, true) {}

                unique_ptr<NodeOutput> execute(const vector<NodeOutput *> &input) override {
                    auto filter = dynamic_cast<MapFilter *>(inner_.get());
                    auto table = static_cast<TableOutput *>(input[0])->get();
                    auto map = static_cast<ItemProcessorOutput *>(input[1]);
                    filter->setMap(map->suppkeys_);
                    auto output = filter->filter(*table);
                    return unique_ptr<NodeOutput>(new TableOutput(output));
                }
            };

            class PartSuppFilter : public NestedNode {
            public:
                PartSuppFilter(PowerMapFilter *inner) : NestedNode(inner, 2, true) {}

                unique_ptr<NodeOutput> execute(const vector<NodeOutput *> &input) override {
                    auto filter = dynamic_cast<PowerMapFilter *>(inner_.get());
                    auto table = static_cast<TableOutput *>(input[0])->get();
                    auto map = static_cast<ItemProcessorOutput *>(input[1]);
                    filter->setMap(map->partsuppkeys_);
                    auto output = filter->filter(*table);
                    return unique_ptr<NodeOutput>(new TableOutput(output));
                }
            };
        };
        using namespace q9;
        using namespace powerjoin;

        void executeQ9() {
            ExecutionGraph graph;

            auto part = graph.add(new TableNode(ParquetTable::Open(Part::path, {Part::PARTKEY, Part::NAME})), {});
            auto partsupp = graph.add(new TableNode(
                    ParquetTable::Open(PartSupp::path, {PartSupp::PARTKEY, PartSupp::SUPPKEY, PartSupp::SUPPLYCOST})),
                                      {});
            auto lineitem = graph.add(new TableNode(ParquetTable::Open(LineItem::path,
                                                                       {LineItem::PARTKEY, LineItem::DISCOUNT,
                                                                        LineItem::EXTENDEDPRICE,
                                                                        LineItem::QUANTITY, LineItem::SUPPKEY,
                                                                        LineItem::ORDERKEY})), {});
            auto order = graph.add(
                    new TableNode(ParquetTable::Open(Orders::path, {Orders::ORDERKEY, Orders::ORDERDATE})), {});
            auto supplier = graph.add(
                    new TableNode(ParquetTable::Open(Supplier::path, {Supplier::NATIONKEY, Supplier::SUPPKEY})), {});
            auto nationTable = ParquetTable::Open(Nation::path, {Nation::NATIONKEY, Nation::NAME});
            auto nation = graph.add(new TableNode(nationTable), {});

            auto partFilter = graph.add(new ColFilter(new SimplePredicate(Part::NAME, [](const DataField &field) {
                auto &ref = field.asByteArray();
                return lqf::util::strnstr((const char *) ref.ptr, "green", ref.len);
            })), {part});

            auto itemPartJoin = graph.add(new FilterJoin(LineItem::PARTKEY, Part::PARTKEY), {lineitem, partFilter});
            auto validItem = graph.add(new FilterMat(), {itemPartJoin});

            // Use the lineitem to make three filter maps
            auto itemProcessor = graph.add(new ItemProcessor(), {validItem});

            auto orderFilter = graph.add(new OrderFilter(new MapFilter(Orders::ORDERKEY)), {order, itemProcessor});

            auto itemOrderJoin = graph.add(
                    new HashJoin(LineItem::ORDERKEY, Orders::ORDERKEY, new ItemWithOrderBuilder()),
                    {validItem, orderFilter});
            // PARTKEY SUPPKEY PRICE_DISCOUNT QUANTITY YEAR

            auto pskey_maker = COL_HASHER2(PartSupp::PARTKEY, PartSupp::SUPPKEY);
            auto itemkey_maker = COL_HASHER2(0, 1);

            auto psFilter = graph.add(new PartSuppFilter(new PowerMapFilter(pskey_maker)), {partsupp, itemProcessor});

            auto ps2lJoin = graph.add(new PowerHashJoin(itemkey_maker, pskey_maker, new ItemWithRevBuilder()),
                                      {itemOrderJoin, psFilter});
            // SUPPKEY, ORDER_YEAR, REV

            auto suppFilter = graph.add(new SupplierFilter(new MapFilter(Supplier::SUPPKEY)),
                                        {supplier, itemProcessor});

            // ORDER_YEAR, REV, NATIONKEY
            auto suppJoin = graph.add(new HashColumnJoin(0, Supplier::SUPPKEY,
                                                         new ColumnBuilder({JL(1), JL(2), JR(Supplier::NATIONKEY)})),
                                      {ps2lJoin, suppFilter});

            auto agg = graph.add(new HashAgg(COL_HASHER2(0, 2),
                                             RowCopyFactory().field(F_REGULAR, 2, 0)
                                                     ->field(F_REGULAR, 0, 1)->buildSnapshot(),
                                             []() { return vector<agg::AggField *>({new agg::DoubleSum(1)}); }),
                                 {suppJoin});
            // NATIONKEY, ORDERYEAR, SUM

            auto withNationJoin = graph.add(new HashJoin(0, Nation::NATIONKEY,
                                                         new RowBuilder({JL(1), JRS(Nation::NAME), JL(2)}, false,
                                                                        true)), {agg, nation});
            // ORDERYEAR, NATION, REV

            function<bool(DataRow *, DataRow *)> comp = [](DataRow *a, DataRow *b) {
                return SBLE(1) || (SBE(1) && SILE(0));
            };
            auto sort = graph.add(new SmallSort(comp), {withNationJoin});

            graph.add(new Printer(PBEGIN PI(0) PB(1) PD(2) PEND), {sort});
            graph.execute();
        }

        void executeQ9Backup() {
            auto part = ParquetTable::Open(Part::path, {Part::PARTKEY, Part::NAME});
            auto partsupp = ParquetTable::Open(PartSupp::path,
                                               {PartSupp::PARTKEY, PartSupp::SUPPKEY, PartSupp::SUPPLYCOST});
            auto lineitem = ParquetTable::Open(LineItem::path,
                                               {LineItem::PARTKEY, LineItem::DISCOUNT, LineItem::EXTENDEDPRICE,
                                                LineItem::QUANTITY, LineItem::SUPPKEY, LineItem::ORDERKEY});
            auto order = ParquetTable::Open(Orders::path, {Orders::ORDERKEY, Orders::ORDERDATE});
            auto supplier = ParquetTable::Open(Supplier::path, {Supplier::NATIONKEY, Supplier::SUPPKEY});
            auto nation = ParquetTable::Open(Nation::path, {Nation::NATIONKEY, Nation::NAME});

            ColFilter partFilter(new SimplePredicate(Part::NAME, [](const DataField &field) {
                auto &ref = field.asByteArray();
                return lqf::util::strnstr((const char *) ref.ptr, "green", ref.len);
            }));
            auto validPart = partFilter.filter(*part);

            FilterMat filterMat;

            FilterJoin itemPartJoin(LineItem::PARTKEY, Part::PARTKEY);
            auto validLineitem = filterMat.mat(*itemPartJoin.join(*lineitem, *validPart));

            // Use the lineitem to make three filter maps
            HashPredicate<Int32> orderkeys;
            HashPredicate<Int32> suppkeys;
            HashPredicate<Int64> partsuppkeys;
            validLineitem->blocks()->foreach([&orderkeys, &suppkeys, &partsuppkeys](const shared_ptr<Block> &block) {
                auto rows = block->rows();
                auto block_size = block->size();
                for (uint32_t i = 0; i < block_size; ++i) {
                    DataRow &row = rows->next();
                    orderkeys.add(row[LineItem::ORDERKEY].asInt());
                    int suppkey = row[LineItem::SUPPKEY].asInt();
                    int partkey = row[LineItem::PARTKEY].asInt();
                    suppkeys.add(suppkey);
                    partsuppkeys.add((static_cast<uint64_t>(partkey) << 32) + suppkey);
                }
            });
//            cout << orderkeys.size() << "," << orderkeys.max() << endl;
//            cout << suppkeys.size() << "," << suppkeys.size() << endl;
//            cout << partsuppkeys.size() << endl;

            MapFilter orderFilter(Orders::ORDERKEY, orderkeys);
            auto filteredOrders = orderFilter.filter(*order);

            HashJoin itemOrderJoin(LineItem::ORDERKEY, Orders::ORDERKEY, new ItemWithOrderBuilder());
            // PARTKEY SUPPKEY PRICE_DISCOUNT QUANTITY YEAR
            auto orderLineitem = itemOrderJoin.join(*validLineitem, *filteredOrders);

            auto pskey_maker = COL_HASHER2(PartSupp::PARTKEY, PartSupp::SUPPKEY);
            auto itemkey_maker = COL_HASHER2(0, 1);

            PowerMapFilter psFilter(pskey_maker, partsuppkeys);
            auto validps = psFilter.filter(*partsupp);

            PowerHashJoin ps2lJoin(itemkey_maker, pskey_maker, new ItemWithRevBuilder());
            // SUPPKEY, ORDER_YEAR, REV
            auto itemWithRev = ps2lJoin.join(*orderLineitem, *validps);

            MapFilter suppFilter(Supplier::SUPPKEY, suppkeys);
            auto validsupp = suppFilter.filter(*supplier);

            // ORDER_YEAR, REV, NATIONKEY
            HashColumnJoin suppJoin(0, Supplier::SUPPKEY, new ColumnBuilder({JL(1), JL(2), JR(Supplier::NATIONKEY)}));
            auto itemWithNation = suppJoin.join(*itemWithRev, *validsupp);

            HashAgg agg(COL_HASHER2(0, 2), RowCopyFactory().field(F_REGULAR, 2, 0)
                                ->field(F_REGULAR, 0, 1)->buildSnapshot(),
                        []() { return vector<agg::AggField *>({new agg::DoubleSum(1)}); });
            // NATIONKEY, ORDERYEAR, SUM
            auto result = agg.agg(*itemWithNation);

            HashJoin withNationJoin(0, Nation::NATIONKEY,
                                    new RowBuilder({JL(1), JRS(Nation::NAME), JL(2)}, false, true));
            // ORDERYEAR, NATION, REV
            auto withNationName = withNationJoin.join(*result, *nation);

            function<bool(DataRow *, DataRow *)> comp = [](DataRow *a, DataRow *b) {
                return SBLE(1) || (SBE(1) && SILE(0));
            };
            SmallSort sort(comp);
            auto sorted = sort.sort(*withNationName);

            Printer printer(PBEGIN PI(0) PB(1) PD(2) PEND);
            printer.print(*sorted);
        }
    }
}