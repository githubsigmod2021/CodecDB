CREATE TABLE lineitem
(
    l_orderkey      INTEGER,
    l_partkey       INTEGER,
    l_suppkey       INTEGER,
    l_linenumber    INTEGER,
    l_quantity      INTEGER,
    l_extendedprice DOUBLE,
    l_discount      DOUBLE,
    l_tax           DOUBLE,
    l_returnflag    CHAR(1),
    l_linestatus    CHAR(1),
    l_shipdate      CHAR(10),
    l_commitdate    CHAR(10),
    l_receiptdate   CHAR(10),
    l_shipinstruct  VARCHAR(50),
    l_shipmode      VARCHAR(10),
    l_comment       VARCHAR(256)
)
WITH (
    format='PARQUET',
    external_location = 'file:///local/hajiang/tpch/5/presto/lineitem'
);