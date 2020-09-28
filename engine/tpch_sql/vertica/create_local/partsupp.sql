CREATE TABLE partsupp (
    ps_partkey INT,
    ps_suppkey INT,
    ps_availqty INT,
    ps_supplycost FLOAT,
    ps_comment VARBINARY(250)
);

COPY partsupp FROM '/local/hajiang/tpch/20/origin/partsupp.tbl' DELIMITER '|' DIRECT;