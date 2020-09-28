SELECT 
    f2.value / f1.value r1, 
    f3.value / f1.value r2, 
    f4.value / f1.value r3, 
    f11.value / f1.value r4, 
    f12.value / f1.value r5, 
    f13.value / f1.value r6, 
    f14.value / f1.value r7, 
    f21.value / f1.value r8, 
    f22.value / f1.value r9, 
    f23.value / f1.value r10, 
    f24.value / f1.value r11, 
    f31.value / f1.value r12, 
    f32.value / f1.value r13, 
    f33.value / f1.value r14,
    f34.value / f1.value r15
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
        AND f3.type = 'EncFileSize'
        AND f3.name = 'DELTA_file_size'
        JOIN
    feature f4 ON f4.col_id = cd.id
        AND f4.type = 'EncFileSize'
        AND f4.name = 'DELTAL_file_size'
        JOIN
    feature f11 ON f11.col_id = cd.id
        AND f11.type = 'CompressEncFileSize'
        AND f11.name = 'PLAIN_GZIP_file_size'
        JOIN
    feature f12 ON f12.col_id = cd.id
        AND f12.type = 'CompressEncFileSize'
        AND f12.name = 'DICT_GZIP_file_size'
        JOIN
    feature f13 ON f13.col_id = cd.id
        AND f13.type = 'CompressEncFileSize'
        AND f13.name = 'DELTA_GZIP_file_size'
        JOIN
    feature f14 ON f14.col_id = cd.id
        AND f14.type = 'CompressEncFileSize'
        AND f14.name = 'DELTAL_GZIP_file_size'
        JOIN
    feature f21 ON f21.col_id = cd.id
        AND f21.type = 'CompressEncFileSize'
        AND f21.name = 'PLAIN_SNAPPY_file_size'
        JOIN
    feature f22 ON f22.col_id = cd.id
        AND f22.type = 'CompressEncFileSize'
        AND f22.name = 'DICT_SNAPPY_file_size'
        JOIN
    feature f23 ON f23.col_id = cd.id
        AND f23.type = 'CompressEncFileSize'
        AND f23.name = 'DELTA_SNAPPY_file_size'
        JOIN
    feature f24 ON f24.col_id = cd.id
        AND f24.type = 'CompressEncFileSize'
        AND f24.name = 'DELTAL_SNAPPY_file_size'
        JOIN
    feature f31 ON f31.col_id = cd.id
        AND f31.type = 'CompressEncFileSize'
        AND f31.name = 'PLAIN_LZO_file_size'
        JOIN
    feature f32 ON f32.col_id = cd.id
        AND f32.type = 'CompressEncFileSize'
        AND f32.name = 'DICT_LZO_file_size'
        JOIN
    feature f33 ON f33.col_id = cd.id
        AND f33.type = 'CompressEncFileSize'
        AND f33.name = 'DELTA_LZO_file_size'
        JOIN
    feature f34 ON f34.col_id = cd.id
        AND f34.type = 'CompressEncFileSize'
        AND f34.name = 'DELTAL_LZO_file_size'
WHERE
    cd.data_type = 'STRING'