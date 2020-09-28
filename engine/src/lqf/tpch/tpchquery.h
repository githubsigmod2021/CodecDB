//
// Created by harper on 3/3/20.
//

#ifndef ARROW_TPCHQUERY_H
#define ARROW_TPCHQUERY_H

#include <string>
#include <sstream>
#include <parquet/types.h>

using namespace std;

namespace lqf {
    namespace tpch {

        class LineItem {
        public:
            static const string path;
            static const int ORDERKEY = 0;
            static const int PARTKEY = 1;
            static const int SUPPKEY = 2;
            static const int LINENUMBER = 3;
            static const int QUANTITY = 4;
            static const int EXTENDEDPRICE = 5;
            static const int DISCOUNT = 6;
            static const int TAX = 7;
            static const int RETURNFLAG = 8;
            static const int LINESTATUS = 9;
            static const int SHIPDATE = 10;
            static const int COMMITDATE = 11;
            static const int RECEIPTDATE = 12;
            static const int SHIPINSTRUCT = 13;
            static const int SHIPMODE = 14;
            static const int COMMENT = 15;
        };

        class Part {
        public:
            static const string path;
            static const int PARTKEY = 0;
            static const int NAME = 1;
            static const int MFGR = 2;
            static const int BRAND = 3;
            static const int TYPE = 4;
            static const int SIZE = 5;
            static const int CONTAINER = 6;
            static const int RETAILPRICE = 7;
            static const int COMMENT = 8;
        };

        class PartSupp {
        public:
            static const string path;
            static const int PARTKEY = 0;
            static const int SUPPKEY = 1;
            static const int AVAILQTY = 2;
            static const int SUPPLYCOST = 3;
            static const int COMMENT = 4;
        };

        class Supplier {
        public:
            static const string path;
            static const int SUPPKEY = 0;
            static const int NAME = 1;
            static const int ADDRESS = 2;
            static const int NATIONKEY = 3;
            static const int PHONE = 4;
            static const int ACCTBAL = 5;
            static const int COMMENT = 6;
        };

        class Nation {
        public:
            static const string path;
            static const int NATIONKEY = 0;
            static const int NAME = 1;
            static const int REGIONKEY = 2;
            static const int COMMENT = 3;
        };

        class Region {
        public:
            static const string path;
            static const int REGIONKEY = 0;
            static const int NAME = 1;
            static const int COMMENT = 2;
        };

        class Customer {
        public:
            static const string path;
            static const int CUSTKEY = 0;
            static const int NAME = 1;
            static const int ADDRESS = 2;
            static const int NATIONKEY = 3;
            static const int PHONE = 4;
            static const int ACCTBAL = 5;
            static const int MKTSEGMENT = 6;
            static const int COMMENT = 7;
        };

        class Orders {
        public:
            static const string path;
            static const int ORDERKEY = 0;
            static const int CUSTKEY = 1;
            static const int ORDERSTATUS = 2;
            static const int TOTALPRICE = 3;
            static const int ORDERDATE = 4;
            static const int ORDERPRIORITY = 5;
            static const int CLERK = 6;
            static const int SHIPPRIORITY = 7;
            static const int COMMENT = 8;
        };

        void executeQ1();
        void executeQ2();
        void executeQ3();
        void executeQ4();
        void executeQ5();
        void executeQ6();
        void executeQ7();
        void executeQ8();
        void executeQ9();
        void executeQ10();
        void executeQ11();
        void executeQ12();
        void executeQ13();
        void executeQ14();
        void executeQ15();
        void executeQ16();
        void executeQ17();
        void executeQ18();
        void executeQ19();
        void executeQ20();
        void executeQ21();
        void executeQ22();
    }

    namespace udf {
        int date2year(parquet::ByteArray &);
    }
}
#endif //ARROW_TPCHQUERY_H
