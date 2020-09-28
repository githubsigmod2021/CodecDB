CREATE TABLE part (
    p_partkey INTEGER,
    p_name VARCHAR(100),
    p_mfgr VARCHAR(20),
    p_brand VARCHAR(20),
    p_type VARCHAR(30),
    p_size INTEGER,
    p_container VARCHAR(20),
    p_retailprice DOUBLE,
    p_comment VARCHAR(250)
);
COPY INTO part FROM('/local/hajiang/tpch/20/origin/part.tbl');
