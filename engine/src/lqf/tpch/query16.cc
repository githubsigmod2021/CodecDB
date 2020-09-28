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

        using namespace agg;
        using namespace sboost;
        using namespace raw;
        namespace q16 {
            ByteArray brand("Brand#45");
            const char *type = "MEDIUM POLISHED";
            unordered_set<int32_t> size{49, 14, 23, 45, 19, 3, 36, 9};
        }

        using namespace q16;

        void executeQ16() {

            ExecutionGraph graph;

            auto part = graph.add(new TableNode(
                    ParquetTable::Open(Part::path, {Part::PARTKEY, Part::BRAND, Part::TYPE, Part::SIZE})), {});

            auto supplier = graph.add(
                    new TableNode(ParquetTable::Open(Supplier::path, {Supplier::SUPPKEY, Supplier::COMMENT})), {});
            auto partsupp = graph.add(
                    new TableNode(ParquetTable::Open(PartSupp::path, {PartSupp::SUPPKEY, PartSupp::PARTKEY})), {});

            function<unique_ptr<RawAccessor<ByteArrayType>>()> brandgen = bind(&ByteArrayDictEq::build, brand);
            function<unique_ptr<RawAccessor<ByteArrayType>>()> typegen = bind(&ByteArrayDictMultiEq::build,
                                                                              [](const ByteArray &field) {
                                                                                  const char *start = (const char *) field.ptr;
                                                                                  return start ==
                                                                                         lqf::util::strnstr(start, type,
                                                                                                            field.len);
                                                                              });
            auto partFilter = graph.add(new ColFilter(
                    {new SboostPredicate<ByteArrayType>(Part::BRAND, bind(&ByteArrayNot::build, brandgen)),
                     new SboostPredicate<Int32Type>(Part::SIZE,
                                                    bind(&Int32DictMultiEq::build, [](const int32_t &field) {
                                                        return size.find(field) != size.end();
                                                    })),
                     new SboostPredicate<ByteArrayType>(Part::TYPE, bind(&ByteArrayNot::build, typegen))}), {part});

            auto supplierFilter = graph.add(
                    new ColFilter(new SimplePredicate(Supplier::COMMENT, [](const DataField &field) {
                        ByteArray &comment = field.asByteArray();
                        const char *start = (const char *) comment.ptr;
                        char *index = lqf::util::strnstr(start, "Customer", comment.len);
                        if (index != NULL) {
                            return lqf::util::strnstr(index, "Complaints", comment.len - 8 - (index - start)) != NULL;
                        }
                        return false;
                    })), {supplier});

            auto psSupplierFilter_obj = new FilterJoin(PartSupp::SUPPKEY, Supplier::SUPPKEY);
            psSupplierFilter_obj->useAnti();
            auto psSupplierFilter = graph.add(psSupplierFilter_obj, {partsupp, supplierFilter});

            auto pswithsJoin = graph.add(
                    new HashJoin(PartSupp::PARTKEY, Part::PARTKEY, new RowBuilder({JL(PartSupp::SUPPKEY),
                                                                                   JRR(Part::BRAND), JRR(Part::TYPE),
                                                                                   JRR(Part::SIZE)})),
                    {psSupplierFilter, partFilter});
            // TODO Which is on the right ?
            // SUPPKEY, BRAND, TYPE, SIZE


            function<uint64_t(DataRow &)> hasher = [](DataRow &input) {
                return (input[1].asInt() << 20) + (input[2].asInt() << 10) + input[3].asInt();
            };
            auto psagg = graph.add(new HashLargeAgg(hasher,
                                               RowCopyFactory().field(F_REGULAR, 1, 0)->field(F_REGULAR, 2, 1)
                                                       ->field(F_REGULAR, 3, 2)->from(RAW)
                                                       ->to(RAW)->from_layout(colOffset(4))->to_layout(
                                                               colOffset(3))->buildSnapshot(),
                                               []() { return vector<AggField *>({new IntDistinctCount(0)}); }),
                                   {pswithsJoin});

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return SILE(3) || (SIE(3) && SILE(0)) || (SIE(3) && SIE(0) && SILE(1)) ||
                       (SIE(3) && SIE(0) && SIE(1) && SILE(2));
            };
            auto sort = graph.add(new SmallSort(comparator), {psagg});

            graph.add(new Printer(PBEGIN PI(0) PI(1) PI(2) PI(3) PEND), {sort});

            graph.execute();
        }

        void executeQ16Backup() {
            auto part = ParquetTable::Open(Part::path, {Part::PARTKEY, Part::BRAND, Part::TYPE, Part::SIZE});
            auto supplier = ParquetTable::Open(Supplier::path, {Supplier::SUPPKEY, Supplier::COMMENT});
            auto partsupp = ParquetTable::Open(PartSupp::path, {PartSupp::SUPPKEY, PartSupp::PARTKEY});

            function<unique_ptr<RawAccessor<ByteArrayType>>()> brandgen = bind(&ByteArrayDictEq::build, brand);
            function<unique_ptr<RawAccessor<ByteArrayType>>()> typegen = bind(&ByteArrayDictMultiEq::build,
                                                                              [](const ByteArray &field) {
                                                                                  const char *start = (const char *) field.ptr;
                                                                                  return start ==
                                                                                         lqf::util::strnstr(start, type,
                                                                                                            field.len);
                                                                              });
            ColFilter partFilter(
                    {new SboostPredicate<ByteArrayType>(Part::BRAND, bind(&ByteArrayNot::build, brandgen)),
                     new SboostPredicate<Int32Type>(Part::SIZE,
                                                    bind(&Int32DictMultiEq::build, [](const int32_t &field) {
                                                        return size.find(field) != size.end();
                                                    })),
                     new SboostPredicate<ByteArrayType>(Part::TYPE, bind(&ByteArrayNot::build, typegen))});
            auto validPart = FilterMat().mat(*partFilter.filter(*part));

            ColFilter supplierFilter({new SimplePredicate(Supplier::COMMENT, [](const DataField &field) {
                ByteArray &comment = field.asByteArray();
                const char *start = (const char *) comment.ptr;
                char *index = lqf::util::strnstr(start, "Customer", comment.len);
                if (index != NULL) {
                    return lqf::util::strnstr(index, "Complaints", comment.len - 8 - (index - start)) != NULL;
                }
                return false;
            })});
            auto validSupplier = supplierFilter.filter(*supplier);

            FilterJoin psSupplierFilter(PartSupp::SUPPKEY, Supplier::SUPPKEY, 50);
            psSupplierFilter.useAnti();
            auto validps = psSupplierFilter.join(*partsupp, *validSupplier);

//            cout << validps->size() << endl;
//            cout << validPart->size() << endl;
            HashJoin pswithsJoin(PartSupp::PARTKEY, Part::PARTKEY, new RowBuilder({JL(PartSupp::SUPPKEY),
                                                                                   JRR(Part::BRAND), JRR(Part::TYPE),
                                                                                   JRR(Part::SIZE)}));
//            // TODO Which is on the right ?
//            // SUPPKEY, BRAND, TYPE, SIZE
            auto partWithSupplier = pswithsJoin.join(*validps, *validPart);
//            cout << partWithSupplier->size();
//
            function<uint64_t(DataRow &)> hasher = [](DataRow &input) {
                return (input[1].asInt() << 20) + (input[2].asInt() << 10) + input[3].asInt();
            };
            HashAgg psagg(hasher,
                          RowCopyFactory().field(F_REGULAR, 1, 0)->field(F_REGULAR, 2, 1)
                                  ->field(F_REGULAR, 3, 2)->from(RAW)
                                  ->to(RAW)->from_layout(colOffset(4))->to_layout(
                                          colOffset(3))->buildSnapshot(),
                          []() { return vector<AggField *>({new IntDistinctCount(0)}); });
            auto result = psagg.agg(*partWithSupplier);

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return SILE(3) || (SIE(3) && SILE(0)) || (SIE(3) && SIE(0) && SILE(1)) ||
                       (SIE(3) && SIE(0) && SIE(1) && SILE(2));
            };
            SmallSort sort(comparator);
            auto sorted = sort.sort(*result);

            Printer printer(PBEGIN PI(0) PI(1) PI(2) PI(3) PEND);
            printer.print(*sorted);
        }
    }
}