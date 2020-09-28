CREATE TABLE customer (
    c_custkey INTEGER,
    c_name VARCHAR(30),
    c_address VARCHAR(100),
    c_nationkey INTEGER,
    c_phone VARCHAR(30),
    c_acctbal DOUBLE,
    c_mktsegment VARCHAR(30),
    c_comment VARCHAR(150)
) WITH (
    format='PARQUET',
    external_location = 'file:///local/hajiang/tpch/5/presto/customer'
);