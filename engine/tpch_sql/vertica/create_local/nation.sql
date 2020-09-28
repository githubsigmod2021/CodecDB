CREATE TABLE nation (
    n_nationkey INT,
    n_name      VARBINARY(20),
    n_regionkey INT,
    n_comment   VARBINARY(250)
);

COPY nation FROM '/local/hajiang/tpch/20/origin/nation.tbl' DELIMITER '|' DIRECT;