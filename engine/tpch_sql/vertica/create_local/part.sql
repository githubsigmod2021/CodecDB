CREATE TABLE part (
                                   p_partkey INT,
                                   p_name VARBINARY(20),
                                   p_mfgr VARBINARY(20),
                                   p_brand VARBINARY(20),
                                   p_type VARBINARY(30),
                                   p_size INT,
                                   p_container VARBINARY(20),
                                   p_retailprice FLOAT,
                                   p_comment VARBINARY(250)
);

COPY part FROM '/local/hajiang/tpch/20/origin/part.tbl' DELIMITER '|' DIRECT;