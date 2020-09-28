//
// Created by harper on 3/25/20.
//
#include <parquet/types.h>
#include "tpchquery.h"

namespace lqf {
    namespace tpch {

        static const string tablePath(const string &name) {
            std::ostringstream stringStream;
            stringStream << "/local/hajiang/tpch/20/presto/" << name << "/" << name << ".parquet";
            return stringStream.str();
        }

        const string LineItem::path = tablePath("lineitem");

        const string Part::path = tablePath("part");

        const string PartSupp::path = tablePath("partsupp");

        const string Supplier::path = tablePath("supplier");

        const string Nation::path = tablePath("nation");

        const string Region::path = tablePath("region");

        const string Customer::path = tablePath("customer");

        const string Orders::path = tablePath("orders");
    }

    namespace udf {
        int date2year(parquet::ByteArray &date) {
            return (date.ptr[0] - '0') * 1000 + (date.ptr[1] - '0') * 100 + (date.ptr[2] - '0') * 10 + date.ptr[3] -
                   '0';
        }
    }
}