CREATE TABLE nation (
    n_nationkey INTEGER,
    n_name      VARCHAR(20),
    n_regionkey INTEGER,
    n_comment   VARCHAR(250)
) WITH (
    format='PARQUET',
    external_location = 'file:///local/hajiang/tpch/5/presto/nation'
);