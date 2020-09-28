CREATE TABLE partsupp (
    ps_partkey INTEGER,
    ps_suppkey INTEGER,
    ps_availqty INTEGER,
    ps_supplycost DOUBLE,
    ps_comment VARCHAR(250)
)
WITH (
    format='PARQUET',
    external_location = 'file:///local/hajiang/tpch/5/presto/partsupp'
);