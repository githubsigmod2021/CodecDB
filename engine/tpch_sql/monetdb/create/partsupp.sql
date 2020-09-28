CREATE TABLE partsupp (
    ps_partkey INTEGER,
    ps_suppkey INTEGER,
    ps_availqty INTEGER,
    ps_supplycost DOUBLE,
    ps_comment VARCHAR(250)
);
COPY INTO partsupp FROM('/local/hajiang/tpch/20/origin/partsupp.tbl');
