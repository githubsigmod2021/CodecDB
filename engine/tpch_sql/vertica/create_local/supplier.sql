CREATE TABLE supplier (
    s_suppkey INT,
    s_name VARBINARY(30),
    s_address VARBINARY(150),
    s_nationkey INT,
    s_phone VARBINARY(20),
    s_acctbal FLOAT,
    s_comment VARBINARY(250)
);

COPY supplier FROM '/local/hajiang/tpch/20/origin/supplier.tbl' DELIMITER '|' DIRECT;