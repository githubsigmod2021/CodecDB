CREATE TABLE supplier (
    s_suppkey INTEGER,
    s_name VARCHAR(30),
    s_address VARCHAR(150),
    s_nationkey INTEGER,
    s_phone VARCHAR(20),
    s_acctbal DOUBLE,
    s_comment VARCHAR(250)
) WITH (
    format='PARQUET',
    external_location = 'file:///local/hajiang/tpch/5/presto/supplier'
);