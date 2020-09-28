//
// Created by Harper on 5/28/20.
//

#include "../threadpool.h"
#include "../data_model.h"
#include "../join.h"
#include "tpchquery.h"
#include <iostream>

using namespace std;

using namespace lqf;
using namespace lqf::tpch;
using namespace lqf::hashcontainer;

class A {
public:
    const static ApplicationVersion version() {
        static ApplicationVersion version("parquet-mr", 1, 2, 9);
        return version;
    }
};

int run_thread() {
    return ApplicationVersion("parquet", 1, 2, 3).VersionLt(ApplicationVersion::PARQUET_816_FIXED_VERSION());
}

int main() {
//    Executor exec(30);
//    for (int i = 0; i < 30; ++i) {
//        exec.submit(run_thread);
//    }
//    ApplicationVersion::PARQUET_816_FIXED_VERSION();
    auto table = ParquetTable::Open(LineItem::path, {LineItem::ORDERKEY, LineItem::SUPPKEY, LineItem::SUPPKEY});
    auto result = HashBuilder::buildHashPredicate(*table, LineItem::SUPPKEY);
}