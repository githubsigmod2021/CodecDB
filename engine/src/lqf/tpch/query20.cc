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

        using namespace sboost;
        using namespace powerjoin;
        using namespace agg;

        namespace q20 {
            ByteArray dateFrom("1994-01-01");
            ByteArray dateTo("1995-01-01");


        }
        using namespace q20;

        void executeQ20() {
            ExecutionGraph graph;
            auto supplier = graph.add(new TableNode(
                    ParquetTable::Open(Supplier::path, {Supplier::NATIONKEY, Supplier::SUPPKEY, Supplier::NAME,
                                                        Supplier::ADDRESS})), {});
            auto lineitem = graph.add(new TableNode(
                    ParquetTable::Open(LineItem::path,
                                       {LineItem::QUANTITY, LineItem::PARTKEY, LineItem::SUPPKEY, LineItem::SHIPDATE})),
                                      {});
            auto part = graph.add(new TableNode(ParquetTable::Open(Part::path, {Part::NAME, Part::PARTKEY})), {});
            auto partsupp = graph.add(new TableNode(ParquetTable::Open(PartSupp::path,
                                                                       {PartSupp::PARTKEY, PartSupp::SUPPKEY,
                                                                        PartSupp::AVAILQTY})), {});


            auto suppFilter = graph.add(new ColFilter(new SboostPredicate<Int32Type>(
                    Supplier::NATIONKEY, bind(&Int32DictEq::build, 3))), {supplier});

            auto matSupplier = graph.add(new FilterMat(), {suppFilter});


            auto partFilter = graph.add(new ColFilter(
                    new SimplePredicate(Part::NAME, [](const DataField &field) {
                        ByteArray &val = field.asByteArray();
                        char *start = (char *) val.ptr;
                        return lqf::util::strnstr(start, "forest", val.len) == start;
                    })), {part});

            auto partfilterPs = graph.add(new FilterJoin(PartSupp::PARTKEY, Part::PARTKEY), {partsupp, partFilter});
//            auto validps = partfilterPs.join(*partsupp, *validpart);

            auto suppFilterPs = graph.add(new FilterJoin(PartSupp::SUPPKEY, Supplier::SUPPKEY),
                                          {partfilterPs, matSupplier});
//            auto validpsWithSupp = suppFilterPs.join(*validps, *validSupplier);
            auto matPs = graph.add(new FilterMat(), {suppFilterPs});
//            auto matPs = fmat.mat(*validpsWithSupp);

            function<int64_t(DataRow &)> key_ext = [](DataRow &input) {
                return (static_cast<int64_t>(input[0].asInt()) << 32) +
                       input[1].asInt();
            };

            auto lineitemFilter = graph.add(new ColFilter(
                    new SboostPredicate<ByteArrayType>(LineItem::SHIPDATE,
                                                       bind(ByteArrayDictRangele::build, dateFrom, dateTo))),
                                            {lineitem});
//            auto validLineitem = lineitemFilter.filter(*lineitem);

            function<int64_t(DataRow &)> key_ext_1 = [](DataRow &input) {
                return (static_cast<int64_t>(input[LineItem::PARTKEY].asInt()) << 32) +
                       input[LineItem::SUPPKEY].asInt();
            };

            auto psFilterItem = graph.add(new PowerHashFilterJoin(key_ext_1, key_ext), {lineitemFilter, matPs});
//            auto lineitemWithPs = psFilterItem.join(*validLineitem, *matPs);

            auto lineitemQuanAgg = graph.add(new HashAgg(
                    COL_HASHER2(LineItem::PARTKEY, LineItem::SUPPKEY),
                    RowCopyFactory().field(F_REGULAR, LineItem::PARTKEY, 0)->field(
                            F_REGULAR, LineItem::SUPPKEY, 1)->buildSnapshot(),
                    []() { return vector<AggField *>{new IntSum(LineItem::QUANTITY)}; }), {psFilterItem});
            // PARTKEY SUPPKEY SUM(QUANTITY)
//            auto agglineitem = lineitemQuanAgg.agg(*lineitemWithPs);

            auto psFilterItem2 = graph.add(new PowerHashJoin(key_ext, key_ext, new RowBuilder({JL(1), JR(2)}, false),
                                                             CONTAINER_SIZE,
                                                             [](DataRow &left, DataRow &right) {
                                                                 return left[PartSupp::AVAILQTY].asInt() >
                                                                        right[0].asInt() * 0.5;
                                                             }), {matPs, lineitemQuanAgg});
            // SUPPKEY
//            auto suppkeys = psFilterItem2.join(*matPs, *agglineitem);

            auto finalFilter = graph.add(new FilterJoin(Supplier::SUPPKEY, 0), {matSupplier, psFilterItem2});
//            auto supplierResult = finalFilter.join(*validSupplier, *suppkeys);

            vector<uint32_t> col_size{2, 2};
            vector<uint32_t> col_offset{0, 2, 4};

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return SBLE(0);
            };
            function<unique_ptr<MemDataRow>(DataRow &)> snapshoter = [&col_offset](DataRow &a) {
                MemDataRow *row = new MemDataRow(col_offset);
                (*row)[0] = a[Supplier::NAME].asByteArray();
                (*row)[1] = a[Supplier::ADDRESS].asByteArray();
                return unique_ptr<MemDataRow>(row);
            };
            auto sort = graph.add(new SnapshotSort(col_size, comparator, snapshoter), {finalFilter});
//            auto sorted = sort.sort(*supplierResult);

            graph.add(new Printer(PBEGIN PB(0) PB(1) PEND), {sort});
//            printer.print(*sorted);
            graph.execute();
        }

        void executeQ20Backup() {

            auto supplier = ParquetTable::Open(Supplier::path, {Supplier::NATIONKEY, Supplier::SUPPKEY, Supplier::NAME,
                                                                Supplier::ADDRESS});
            auto lineitem = ParquetTable::Open(LineItem::path,
                                               {LineItem::QUANTITY, LineItem::PARTKEY, LineItem::SUPPKEY,
                                                LineItem::SHIPDATE});
            auto part = ParquetTable::Open(Part::path, {Part::NAME, Part::PARTKEY});
            auto partsupp = ParquetTable::Open(PartSupp::path,
                                               {PartSupp::PARTKEY, PartSupp::SUPPKEY, PartSupp::AVAILQTY});

            FilterMat fmat;

            ColFilter suppFilter(new SboostPredicate<Int32Type>(Supplier::NATIONKEY,
                                                                bind(&Int32DictEq::build, 3)));
            auto validSupplier = fmat.mat(*suppFilter.filter(*supplier));

            ColFilter partFilter(
                    new SimplePredicate(Part::NAME, [](const DataField &field) {
                        ByteArray &val = field.asByteArray();
                        char *start = (char *) val.ptr;
                        return lqf::util::strnstr(start, "forest", val.len) == start;
                    }));
            auto validpart = partFilter.filter(*part);

            FilterJoin partfilterPs(PartSupp::PARTKEY, Part::PARTKEY);
            auto validps = partfilterPs.join(*partsupp, *validpart);

            FilterJoin suppFilterPs(PartSupp::SUPPKEY, Supplier::SUPPKEY);
            auto validpsWithSupp = suppFilterPs.join(*validps, *validSupplier);
            auto matPs = fmat.mat(*validpsWithSupp);

            function<int64_t(DataRow &)> key_ext = [](DataRow &input) {
                return (static_cast<int64_t>(input[0].asInt()) << 32) +
                       input[1].asInt();
            };

            ColFilter lineitemFilter(new SboostPredicate<ByteArrayType>(LineItem::SHIPDATE,
                                                                        bind(ByteArrayDictRangele::build, dateFrom,
                                                                             dateTo)));
            auto validLineitem = lineitemFilter.filter(*lineitem);

            function<int64_t(DataRow &)> key_ext_1 = [](DataRow &input) {
                return (static_cast<int64_t>(input[LineItem::PARTKEY].asInt()) << 32) +
                       input[LineItem::SUPPKEY].asInt();
            };

            PowerHashFilterJoin psFilterItem(key_ext_1, key_ext);
            auto lineitemWithPs = psFilterItem.join(*validLineitem, *matPs);

            HashAgg lineitemQuanAgg(COL_HASHER2(LineItem::PARTKEY, LineItem::SUPPKEY),
                                    RowCopyFactory().field(F_REGULAR, LineItem::PARTKEY, 0)->field(
                                            F_REGULAR, LineItem::SUPPKEY, 1)->buildSnapshot(),
                                    []() { return vector<AggField *>{new IntSum(LineItem::QUANTITY)}; });
            // PARTKEY SUPPKEY SUM(QUANTITY)
            auto agglineitem = lineitemQuanAgg.agg(*lineitemWithPs);

            PowerHashJoin psFilterItem2(key_ext, key_ext, new RowBuilder({JL(1), JR(2)}, false),
                                        CONTAINER_SIZE,
                                        [](DataRow &left, DataRow &right) {
                                            return left[PartSupp::AVAILQTY].asInt() > right[0].asInt() * 0.5;
                                        });
            // SUPPKEY
            auto suppkeys = psFilterItem2.join(*matPs, *agglineitem);

            FilterJoin finalFilter(Supplier::SUPPKEY, 0);
            auto supplierResult = finalFilter.join(*validSupplier, *suppkeys);

            vector<uint32_t> col_size{2, 2};
            vector<uint32_t> col_offset{0, 2, 4};

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return SBLE(0);
            };
            function<unique_ptr<MemDataRow>(DataRow &)> snapshoter = [&col_offset](DataRow &a) {
                MemDataRow *row = new MemDataRow(col_offset);
                (*row)[0] = a[Supplier::NAME].asByteArray();
                (*row)[1] = a[Supplier::ADDRESS].asByteArray();
                return unique_ptr<MemDataRow>(row);
            };
            SnapshotSort sort(col_size, comparator, snapshoter);
            auto sorted = sort.sort(*supplierResult);

            Printer printer(PBEGIN PB(0) PB(1) PEND);
            printer.print(*sorted);
        }
    }
}