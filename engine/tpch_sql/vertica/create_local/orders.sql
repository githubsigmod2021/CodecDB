CREATE TABLE orders
(
    o_orderkey INT,
    o_custkey INT,
    o_orderstatus VARBINARY(10),
    o_totalprice FLOAT,
    o_orderdate BINARY(10),
    o_orderpriority VARBINARY(10),
    o_clerk VARBINARY(150),
    o_shippriority VARBINARY(5),
    o_comment VARBINARY(250)
);

COPY orders FROM '/local/hajiang/tpch/20/origin/orders.tbl' DELIMITER '|' DIRECT;