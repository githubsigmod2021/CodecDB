CREATE EXTERNAL TABLE supplier (
    s_suppkey INT,
    s_name VARBINARY(30),
    s_address VARBINARY(150),
    s_nationkey INT,
    s_phone VARBINARY(20),
    s_acctbal FLOAT,
    s_comment VARBINARY(250)
) AS COPY FROM '/local/hajiang/tpch/5/vertica/supplier.parquet' PARQUET;