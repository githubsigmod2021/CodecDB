CREATE EXTERNAL TABLE orders
(
    o_orderkey INT,
    o_custkey INT,
    o_orderstatus VARBINARY(10),
    o_totalprice FLOAT,
    o_orderdate BINARY(10),
    o_orderpriority VARBINARY(2),
    o_clerk VARBINARY(150),
    o_shippriority VARBINARY(5),
    o_comment VARBINARY(250)
) AS COPY FROM '/local/hajiang/tpch/5/vertica/orders.parquet' PARQUET;