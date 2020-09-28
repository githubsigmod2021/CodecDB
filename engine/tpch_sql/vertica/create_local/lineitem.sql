CREATE TABLE lineitem (
    l_orderkey INT,
    l_partkey INT,
    l_suppkey INT,
    l_linenumber INT,
    l_quantity INT,
    l_extendedprice FLOAT,
    l_discount FLOAT,
    l_tax FLOAT,
    l_returnflag BINARY(1),
    l_linestatus BINARY(1),
    l_shipdate BINARY(10),
    l_commitdate BINARY(10),
    l_receiptdate BINARY(10),
    l_shipinstruct VARBINARY(50),
    l_shipmode VARBINARY(10),
    l_comment  VARBINARY(256)
);

COPY lineitem FROM '/local/hajiang/tpch/20/origin/lineitem.tbl' DELIMITER '|' DIRECT;