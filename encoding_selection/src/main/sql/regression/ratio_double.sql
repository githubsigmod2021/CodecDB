SELECT 
cd.id,
    f2.value / f1.value r1,
    f3.value / f1.value r2,
    f4.value / f1.value r3,
    f5.value / f1.value r4,
    f6.value / f1.value r5,
    f7.value / f1.value r6,
    f8.value / f1.value r7
FROM
    col_data cd
        JOIN
    feature f1 ON f1.col_id = cd.id
        AND f1.type = 'EncFileSize'
        AND f1.name = 'PLAIN_file_size'
        JOIN
    feature f2 ON f2.col_id = cd.id
        AND f2.type = 'EncFileSize'
        AND f2.name = 'DICT_file_size'
        JOIN
    feature f3 ON f3.col_id = cd.id
        AND f3.type = 'CompressEncFileSize'
        AND f3.name = 'PLAIN_GZIP_file_size'
        JOIN
    feature f4 ON f4.col_id = cd.id
        AND f4.type = 'CompressEncFileSize'
        AND f4.name = 'DICT_GZIP_file_size'
        JOIN
    feature f5 ON f5.col_id = cd.id
        AND f5.type = 'CompressEncFileSize'
        AND f5.name = 'PLAIN_SNAPPY_file_size'
        JOIN
    feature f6 ON f6.col_id = cd.id
        AND f6.type = 'CompressEncFileSize'
        AND f6.name = 'DICT_SNAPPY_file_size'
        JOIN
    feature f7 ON f7.col_id = cd.id
        AND f7.type = 'CompressEncFileSize'
        AND f7.name = 'PLAIN_LZO_file_size'
        JOIN
    feature f8 ON f8.col_id = cd.id
        AND f8.type = 'CompressEncFileSize'
        AND f8.name = 'DICT_LZO_file_size'
WHERE
    cd.data_type = 'DOUBLE'