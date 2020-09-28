CREATE TABLE nation (
    n_nationkey INTEGER,
    n_name      VARCHAR(20),
    n_regionkey INTEGER,
    n_comment   VARCHAR(250)
);
COPY INTO nation FROM('/local/hajiang/tpch/20/origin/nation.tbl');
