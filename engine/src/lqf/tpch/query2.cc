//
// Created by harper on 3/3/20.
//

#include <parquet/types.h>
#include <lqf/data_model.h>
#include <lqf/filter.h>
#include <lqf/join.h>
#include <lqf/mat.h>
#include <lqf/agg.h>
#include <lqf/print.h>
#include <lqf/sort.h>
#include <lqf/rowcopy.h>
#include "tpchquery.h"

namespace lqf {
    namespace tpch {
        namespace q2 {
            ByteArray region("EUROPE");
            int size = 15;
            const char *const type = "BRASS";
        }
        using namespace sboost;
        using namespace parallel;
        using namespace rowcopy;

        void executeQ2_Graph() {
            auto partSuppTable = ParquetTable::Open(PartSupp::path,
                                                    {PartSupp::PARTKEY, PartSupp::SUPPKEY, PartSupp::SUPPLYCOST});
            auto supplierTable = ParquetTable::Open(Supplier::path,
                                                    {Supplier::SUPPKEY, Supplier::NATIONKEY, Supplier::NAME,
                                                     Supplier::ADDRESS, Supplier::COMMENT, Supplier::PHONE,
                                                     Supplier::ACCTBAL});
            auto nationTable = ParquetTable::Open(Nation::path, {Nation::NATIONKEY, Nation::NAME, Nation::REGIONKEY});
            auto regionTable = ParquetTable::Open(Region::path, {Region::REGIONKEY, Region::NAME});
            auto partTable = ParquetTable::Open(Part::path, {Part::PARTKEY, Part::TYPE, Part::SIZE, Part::MFGR});

            ExecutionGraph graph;

            auto partsupp = graph.add(new TableNode(partSuppTable), {});
            auto supplier = graph.add(new TableNode(supplierTable), {});
            auto nation = graph.add(new TableNode(nationTable), {});
            auto region = graph.add(new TableNode(regionTable), {});
            auto part = graph.add(new TableNode(partTable), {});

            function<bool(const ByteArray &)> typePred = [](const ByteArray &input) {
                return !strncmp(reinterpret_cast<const char *>(input.ptr + input.len - 5), q2::type, 5);
            };

            auto partFilter = graph.add(new ColFilter({new SboostPredicate<Int32Type>(Part::SIZE,
                                                                                      bind(sboost::Int32DictEq::build,
                                                                                           q2::size)),
                                                       new SboostPredicate<ByteArrayType>(Part::TYPE,
                                                                                          bind(sboost::ByteArrayDictMultiEq::build,
                                                                                               typePred))}), {part});

            auto regionFilter = graph.add(new ColFilter({new SimplePredicate(Region::NAME, [](const DataField &field) {
                return q2::region == (field.asByteArray());
            })}), {region});

            auto nrJoin = graph.add(new FilterJoin(Nation::REGIONKEY, Region::REGIONKEY), {nation, regionFilter});

            auto nationMat = graph.add(new HashMat(Nation::NATIONKEY, RowCopyFactory()
                    .from(EXTERNAL)->to(RAW)
                    ->field(F_STRING, Nation::NAME, 0)->buildSnapshot()), {nrJoin});

            auto snJoin = graph.add(new FilterJoin(Supplier::NATIONKEY, Nation::NATIONKEY), {supplier, nationMat});

            auto partMat = graph.add(new FilterMat(), {partFilter});
            auto supplierMat = graph.add(new FilterMat(), {snJoin});

            // Sequence of these two joins
            auto pspJoin = graph.add(new FilterJoin(PartSupp::PARTKEY, Part::PARTKEY), {partsupp, partMat});

            auto pssJoin = graph.add(new FilterJoin(PartSupp::SUPPKEY, Supplier::SUPPKEY), {pspJoin, supplierMat});

            auto psMinCost = graph.add(new RecordingHashAgg(COL_HASHER(PartSupp::PARTKEY),
                                                            RowCopyFactory().field(F_REGULAR, PartSupp::PARTKEY,
                                                                                   0)->buildSnapshot(),
                                                            []() {
                                                                return new agg::recording::RecordingDoubleMin(
                                                                        PartSupp::SUPPLYCOST, PartSupp::SUPPKEY);
                                                            }, nullptr, true), {pssJoin});
            // PARTKEY SUPPLYCOST SUPPKEY

            auto ps2partJoin = graph.add(
                    new HashColumnJoin(0, Part::PARTKEY, new ColumnBuilder({JL(0), JL(2), JRS(Part::MFGR)})),
                    {psMinCost, partMat});
            // 0 PARTKEY 1 SUPPKEY 2 P_MFGR

            auto ps2suppJoin = graph.add(new HashColumnJoin(1, Supplier::SUPPKEY, new ColumnBuilder(
                    {JL(0), JR(Supplier::ACCTBAL), JR(Supplier::NATIONKEY), JRS(Supplier::NAME), JRS(Supplier::ADDRESS),
                     JRS(Supplier::PHONE), JRS(Supplier::COMMENT), JLS(2)})), {ps2partJoin, supplierMat});
            // 0 PARTKEY 1 ACCTBAL 2 NATIONKEY 3 SNAME 4 ADDRESS 5 PHONE 6 COMMENT 7 MFGR

            auto psWithNationJoin = graph.add(new HashColumnJoin(2, 0, new ColumnBuilder(
                    {JL(0), JL(1), JLS(3), JLS(4), JLS(5), JLS(6), JLS(7), JRS(0)})), {ps2suppJoin, nationMat});
            // 0 PARTKEY 1 ACCTBAL 2 SNAME 3 ADDRESS 4 PHONE 5 COMMENT 6 MFGR 7 NATIONNAME

            // s_acctbal desc, n_name, s_name, p_partkey
            auto top = graph.add(new TopN(100, [](DataRow *a, DataRow *b) {
                return SDGE(1) || (SDE(1) && SBLE(7)) || (SDE(1) && SBE(7) && SBLE(2)) ||
                       (SDE(1) && SBE(7) && SBE(2) && SILE(0));
            }), {psWithNationJoin});

            graph.add(new Printer(PBEGIN PI(0) PD(1) /*PB(2)*/ PB(3) PB(4) PB(5) PB(6) PB(7) PEND), {top});

            graph.execute();
        }

        void executeQ2() {
            auto partSuppTable = ParquetTable::Open(PartSupp::path,
                                                    {PartSupp::PARTKEY, PartSupp::SUPPKEY, PartSupp::SUPPLYCOST});
            auto supplierTable = ParquetTable::Open(Supplier::path,
                                                    {Supplier::SUPPKEY, Supplier::NATIONKEY, Supplier::NAME,
                                                     Supplier::ADDRESS, Supplier::COMMENT, Supplier::PHONE,
                                                     Supplier::ACCTBAL});
            auto nationTable = ParquetTable::Open(Nation::path, {Nation::NATIONKEY, Nation::NAME, Nation::REGIONKEY});
            auto regionTable = ParquetTable::Open(Region::path, {Region::REGIONKEY, Region::NAME});
            auto partTable = ParquetTable::Open(Part::path, {Part::PARTKEY, Part::TYPE, Part::SIZE, Part::MFGR});

            ColFilter regionFilter({new SimplePredicate(Region::NAME, [](const DataField &field) {
                return q2::region == (field.asByteArray());
            })});
            auto filteredRegion = regionFilter.filter(*regionTable);

            function<bool(const ByteArray &)> typePred = [](const ByteArray &input) {
                return !strncmp(reinterpret_cast<const char *>(input.ptr + input.len - 5), q2::type, 5);
            };

            ColFilter partFilter({new SboostPredicate<Int32Type>(Part::SIZE,
                                                                 bind(sboost::Int32DictEq::build, q2::size)),
                                  new SboostPredicate<ByteArrayType>(Part::TYPE,
                                                                     bind(sboost::ByteArrayDictMultiEq::build,
                                                                          typePred))});
            auto filteredPart = partFilter.filter(*partTable);


            FilterJoin nrJoin(Nation::REGIONKEY, Region::REGIONKEY);
            auto filteredNation = nrJoin.join(*nationTable, *filteredRegion);

            HashMat nationMat(Nation::NATIONKEY, RowCopyFactory()
                    .from(EXTERNAL)->to(RAW)
                    ->field(F_STRING, Nation::NAME, 0)->buildSnapshot());
            auto memNationTable = nationMat.mat(*filteredNation);

            FilterJoin snJoin(Supplier::NATIONKEY, Nation::NATIONKEY);
            auto filteredSupplier = snJoin.join(*supplierTable, *memNationTable);


            FilterMat filterMat;
            auto matPart = filterMat.mat(*filteredPart);
            auto matSupplier = filterMat.mat(*filteredSupplier);

            // Sequence of these two joins
            FilterJoin pspJoin(PartSupp::PARTKEY, Part::PARTKEY);
            auto filteredPs = pspJoin.join(*partSuppTable, *matPart);

            FilterJoin pssJoin(PartSupp::SUPPKEY, Supplier::SUPPKEY);
            auto filteredPss = pssJoin.join(*filteredPs, *matSupplier);

            RecordingHashAgg psAgg(COL_HASHER(PartSupp::PARTKEY),
                                   RowCopyFactory().field(F_REGULAR, PartSupp::PARTKEY, 0)->buildSnapshot(),
                                   []() {
                                       return new agg::recording::RecordingDoubleMin(PartSupp::SUPPLYCOST,
                                                                                     PartSupp::SUPPKEY);
                                   }, nullptr, true);
            // PARTKEY SUPPLYCOST SUPPKEY
            auto psMinCostTable = psAgg.agg(*filteredPss);

            HashColumnJoin ps2partJoin(0, Part::PARTKEY, new ColumnBuilder({JL(0), JL(2), JRS(Part::MFGR)}));
            // 0 PARTKEY 1 SUPPKEY 2 P_MFGR
            auto pswithPartTable = ps2partJoin.join(*psMinCostTable, *matPart);

            HashColumnJoin ps2suppJoin(1, Supplier::SUPPKEY, new ColumnBuilder(
                    {JL(0), JR(Supplier::ACCTBAL), JR(Supplier::NATIONKEY), JRS(Supplier::NAME), JRS(Supplier::ADDRESS),
                     JRS(Supplier::PHONE), JRS(Supplier::COMMENT), JLS(2)}));
            // 0 PARTKEY 1 ACCTBAL 2 NATIONKEY 3 SNAME 4 ADDRESS 5 PHONE 6 COMMENT 7 MFGR
            auto psWithBothTable = ps2suppJoin.join(*pswithPartTable, *matSupplier);

            HashColumnJoin psWithNationJoin(2, 0, new ColumnBuilder(
                    {JL(0), JL(1), JLS(3), JLS(4), JLS(5), JLS(6), JLS(7), JRS(0)}));
            // 0 PARTKEY 1 ACCTBAL 2 SNAME 3 ADDRESS 4 PHONE 5 COMMENT 6 MFGR 7 NATIONNAME
            auto alljoined = psWithNationJoin.join(*psWithBothTable, *memNationTable);

            // s_acctbal desc, n_name, s_name, p_partkey
            TopN top(100, [](DataRow *a, DataRow *b) {
                return SDGE(1) || (SDE(1) && SBLE(7)) || (SDE(1) && SBE(7) && SBLE(2)) ||
                       (SDE(1) && SBE(7) && SBE(2) && SILE(0));
            });;
            auto result = top.sort(*alljoined);

            Printer printer(PBEGIN PI(0) PD(1) /*PB(2)*/ PB(3) PB(4) PB(5) PB(6) PB(7) PEND);
            printer.print(*result);
        }

        void executeQ2Debug() {
//            auto partSuppTable = ParquetTable::Open(PartSupp::path,
//                                                    {PartSupp::PARTKEY, PartSupp::SUPPKEY, PartSupp::SUPPLYCOST});
//            auto supplierTable = ParquetTable::Open(Supplier::path,
//                                                    {Supplier::SUPPKEY, Supplier::NATIONKEY, Supplier::NAME,
//                                                     Supplier::ADDRESS, Supplier::COMMENT, Supplier::PHONE,
//                                                     Supplier::ACCTBAL});
            auto nationTable = ParquetTable::Open(Nation::path, {Nation::NATIONKEY, Nation::NAME, Nation::REGIONKEY});
            auto regionTable = ParquetTable::Open(Region::path, {Region::REGIONKEY, Region::NAME});
            auto partTable = ParquetTable::Open(Part::path, {Part::PARTKEY, Part::TYPE, Part::SIZE, Part::MFGR});

            function<bool(const ByteArray &)> typePred = [](const ByteArray &input) {
                return !strncmp(reinterpret_cast<const char *>(input.ptr + input.len - 5), q2::type, 5);
            };

            ColFilter partFilter({new SboostPredicate<Int32Type>(Part::SIZE,
                                                                 bind(sboost::Int32DictEq::build, q2::size)),
                                  new SboostPredicate<ByteArrayType>(Part::TYPE,
                                                                     bind(sboost::ByteArrayDictMultiEq::build,
                                                                          typePred))});
            auto filteredPart = partFilter.filter(*partTable);
            cout << filteredPart->size() << endl;

//            ColFilter regionFilter({new SimplePredicate(Region::NAME, [](const DataField &field) {
//                return q2::region == (field.asByteArray());
//            })});
//            auto filteredRegion = regionFilter.filter(*regionTable);
//            FilterJoin nrJoin(Nation::REGIONKEY, Region::REGIONKEY);
//            auto filteredNation = nrJoin.join(*nationTable, *filteredRegion);
//
//            HashMat nationMat(Nation::NATIONKEY, RowCopyFactory()
//                    .from(I_EXTERNAL)->to(I_RAW)
//                    ->field(F_STRING, Nation::NAME, 0)->buildSnapshot());
//            auto memNationTable = nationMat.mat(*filteredNation);
//
//            FilterJoin snJoin(Supplier::NATIONKEY, Nation::NATIONKEY);
//            auto filteredSupplier = snJoin.join(*supplierTable, *memNationTable);
//
//            cout << filteredSupplier->size() << endl;
        }
    }
}