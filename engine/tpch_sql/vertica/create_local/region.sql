CREATE TABLE region (
    r_regionkey INT,
    r_name VARBINARY(15),
    r_comment VARBINARY(250)
);

COPY region FROM '/local/hajiang/tpch/20/origin/region.tbl' DELIMITER '|' DIRECT;