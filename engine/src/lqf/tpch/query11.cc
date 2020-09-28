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

        namespace q11 {

            ByteArray nationChosen("GERMANY");
            double fraction = 0.0001;

            class CostField : public agg::DoubleSum {
            public:
                CostField() : agg::DoubleSum(0) {}

                void reduce(DataRow &row) {
                    value_ = value_.asDouble() + row[PartSupp::AVAILQTY].asInt() * row[PartSupp::SUPPLYCOST].asDouble();
                }
            };

            class TotalAggNode : public SimpleAgg {
            public:
                TotalAggNode(function<vector<agg::AggField *>()> field_gen) : SimpleAgg(field_gen) {}

                unique_ptr<NodeOutput> execute(const vector<NodeOutput *> &input) override {
                    auto input0 = static_cast<TableOutput *>(input[0]);
                    auto result = agg(*(input0->get()));
                    double total_value = (*(*result->blocks()->collect())[0]->rows())[0][0].asDouble();
                    double threshold = total_value * fraction;
                    return unique_ptr<TypedOutput<double>>(new TypedOutput(threshold));
                }
            };

            class PartAggNode : public NestedNode {
            public:
                PartAggNode(Node *inner) : NestedNode(inner, 2) {}

                unique_ptr<NodeOutput> execute(const vector<NodeOutput *> &input) override {
                    auto inneragg = dynamic_cast<HashAgg *>(inner_.get());
                    auto total = (static_cast<TypedOutput<double> *>(input[0]))->get();
                    inneragg->setPredicate([=](DataRow &input) {
                        return input[1].asDouble() >= total;
                    });
                    auto input0 = static_cast<TableOutput *>(input[1]);
                    auto result = inneragg->agg(*(input0->get()));
                    return unique_ptr<TableOutput>(new TableOutput(result));
                }
            };
        }
        using namespace q11;

        void executeQ11_Graph() {

            ExecutionGraph graph;

            auto nation = graph.add(new TableNode(ParquetTable::Open(Nation::path, {Nation::NATIONKEY, Nation::NAME})),
                                    {});
            auto supplier = graph.add(
                    new TableNode(ParquetTable::Open(Supplier::path, {Supplier::NATIONKEY, Supplier::SUPPKEY})), {});
            auto partsupp = graph.add(new TableNode(ParquetTable::Open(PartSupp::path,
                                                                       {PartSupp::SUPPKEY, PartSupp::PARTKEY,
                                                                        PartSupp::AVAILQTY,
                                                                        PartSupp::SUPPLYCOST})), {});
            auto nationNameFilter = graph.add(
                    new ColFilter({new SimplePredicate(Nation::NAME, [=](const DataField &field) {
                        return field.asByteArray() == nationChosen;
                    })}), {nation});

            auto validSupplierJoin = graph.add(new FilterJoin(Supplier::NATIONKEY, Nation::NATIONKEY),
                                               {supplier, nationNameFilter});

            auto validPsJoin = graph.add(new FilterJoin(PartSupp::SUPPKEY, Supplier::SUPPKEY),
                                         {partsupp, validSupplierJoin});

            auto matPs = graph.add(new FilterMat(), {validPsJoin});

            function<vector<agg::AggField *>()> agg_fields = []() { return vector<agg::AggField *>{new CostField()}; };
            auto totalAgg = graph.add(new TotalAggNode(agg_fields), {matPs});


            auto bypartAgg = graph.add(new PartAggNode(new HashAgg(
                    COL_HASHER(PartSupp::PARTKEY),
                    RowCopyFactory().field(F_REGULAR, PartSupp::PARTKEY, 0)->buildSnapshot(),
                    agg_fields)), {totalAgg, matPs});

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) { return SDGE(1); };
            auto sort = graph.add(new SmallSort(comparator), {bypartAgg});

            graph.add(new Printer(PBEGIN PI(0) PD(1) PEND), {sort});

            graph.execute();
        }

        void executeQ11() {

            auto nation = ParquetTable::Open(Nation::path, {Nation::NATIONKEY, Nation::NAME});
            auto supplier = ParquetTable::Open(Supplier::path, {Supplier::NATIONKEY, Supplier::SUPPKEY});
            auto partsupp = ParquetTable::Open(PartSupp::path,
                                               {PartSupp::SUPPKEY, PartSupp::PARTKEY, PartSupp::AVAILQTY,
                                                PartSupp::SUPPLYCOST});

            ColFilter nationNameFilter({new SimplePredicate(Nation::NAME, [=](const DataField &field) {
                return field.asByteArray() == nationChosen;
            })});
            auto validNation = nationNameFilter.filter(*nation);

            FilterJoin validSupplierJoin(Supplier::NATIONKEY, Nation::NATIONKEY);
            auto validSupplier = validSupplierJoin.join(*supplier, *validNation);

            FilterJoin validPsJoin(PartSupp::SUPPKEY, Supplier::SUPPKEY);
            auto validps = FilterMat().mat(*validPsJoin.join(*partsupp, *validSupplier));

            function<vector<agg::AggField *>()> agg_fields = []() { return vector<agg::AggField *>{new CostField()}; };
            SimpleAgg totalAgg(agg_fields);
            auto total = totalAgg.agg(*validps);
            double total_value = (*(*total->blocks()->collect())[0]->rows())[0][0].asDouble();
            double threshold = total_value * fraction;

            HashAgg bypartAgg(COL_HASHER(PartSupp::PARTKEY),
                              RowCopyFactory().field(F_REGULAR, PartSupp::PARTKEY, 0)->buildSnapshot(),
                              agg_fields, [=](DataRow &input) {
                        return input[1].asDouble() >= threshold;
                    });
            auto byParts = bypartAgg.agg(*validps);

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) { return SDGE(1); };
            SmallSort sort(comparator);
            auto sorted = sort.sort(*byParts);

            Printer printer(PBEGIN PI(0) PD(1) PEND);
            printer.print(*sorted);
        }
    }
}