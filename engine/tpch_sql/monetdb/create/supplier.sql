CREATE TABLE supplier (
    s_suppkey INTEGER,
    s_name VARCHAR(30),
    s_address VARCHAR(150),
    s_nationkey INTEGER,
    s_phone VARCHAR(20),
    s_acctbal DOUBLE,
    s_comment VARCHAR(250)
);
COPY INTO supplier FROM('/local/hajiang/tpch/20/origin/supplier.tbl');
