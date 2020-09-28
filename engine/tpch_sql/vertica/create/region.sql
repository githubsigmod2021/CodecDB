CREATE EXTERNAL TABLE region (
    r_regionkey INT,
    r_name VARBINARY(15),
    r_comment VARBINARY(250)
) AS COPY FROM '/local/hajiang/tpch/5/vertica/region.parquet' PARQUET;