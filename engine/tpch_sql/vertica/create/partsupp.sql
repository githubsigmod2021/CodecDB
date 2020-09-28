CREATE EXTERNAL TABLE partsupp (
    ps_partkey INT,
    ps_suppkey INT,
    ps_availqty INT,
    ps_supplycost FLOAT,
    ps_comment VARBINARY(250)
) AS COPY FROM '/local/hajiang/tpch/5/vertica/partsupp.parquet' PARQUET;