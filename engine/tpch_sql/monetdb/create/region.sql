CREATE TABLE region (
    r_regionkey INTEGER,
    r_name VARCHAR(15),
    r_comment VARCHAR(250)
);
COPY INTO region FROM('/local/hajiang/tpch/20/origin/region.tbl');
