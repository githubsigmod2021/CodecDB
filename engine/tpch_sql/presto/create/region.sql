CREATE TABLE region (
    r_regionkey INTEGER,
    r_name VARCHAR(15),
    r_comment VARCHAR(250)
)
WITH (
    format='PARQUET',
    external_location = 'file:///local/hajiang/tpch/5/presto/region'
);