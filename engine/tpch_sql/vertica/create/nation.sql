CREATE EXTERNAL TABLE nation (
    n_nationkey INT,
    n_name      VARBINARY(20),
    n_regionkey INT,
    n_comment   VARBINARY(250)
) AS COPY FROM '/local/hajiang/tpch/5/vertica/nation.parquet' PARQUET;