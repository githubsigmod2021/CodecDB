CREATE TABLE orders
(
    o_orderkey INTEGER,
    o_custkey INTEGER,
    o_orderstatus VARCHAR(10),
    o_totalprice DOUBLE,
    o_orderdate CHAR(10),
    o_orderpriority VARCHAR(15),
    o_clerk VARCHAR(150),
    o_shippriority VARCHAR(5),
    o_comment VARCHAR(250)
);
COPY INTO orders FROM('/local/hajiang/tpch/20/origin/orders.tbl');
