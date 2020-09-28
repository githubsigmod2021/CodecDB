CREATE TABLE orders
(
    o_orderkey INTEGER,
    o_custkey INTEGER,
    o_orderstatus VARCHAR(10),
    o_totalprice DOUBLE,
    o_orderdate CHAR(10),
    o_orderpriority VARCHAR(2),
    o_clerk VARCHAR(150),
    o_shippriority VARCHAR(5),
    o_comment VARCHAR(250)
)
WITH (
    format='PARQUET',
    external_location = 'file:///local/hajiang/tpch/5/presto/orders'
);