SELECT 
	cd.id id,
    f2.value / f1.value r1,
    f3.value / f1.value r2,
    f4.value / f1.value r3,
    f5.value / f1.value r4,
    f6.value / f1.value r5,
    f7.value / f1.value r6,
    f8.value / f1.value r7,
    f9.value / f1.value r8,
    f10.value / f1.value r9,
    f11.value / f1.value r10,
    f12.value / f1.value r11,
    f13.value / f1.value r12,
    f14.value / f1.value r13,
    f15.value / f1.value r14,
    f16.value / f1.value r15,
    f17.value / f1.value r16,
    f18.value / f1.value r17,
    f19.value / f1.value r18,
    f20.value / f1.value r19
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
        AND f3.name = 'RLE_file_size'
        JOIN
    feature f4 ON f4.col_id = cd.id
        AND f4.type = 'EncFileSize'
        AND f4.name = 'BP_file_size'
        JOIN
    feature f5 ON f5.col_id = cd.id
        AND f5.type = 'EncFileSize'
        AND f5.name = 'DELTABP_file_size'
        JOIN
    feature f6 ON f6.col_id = cd.id
        AND f6.type = 'CompressEncFileSize'
        AND f6.name = 'PLAIN_GZIP_file_size'
        JOIN
    feature f7 ON f7.col_id = cd.id
        AND f7.type = 'CompressEncFileSize'
        AND f7.name = 'DICT_GZIP_file_size'
        JOIN
    feature f8 ON f8.col_id = cd.id
        AND f8.type = 'CompressEncFileSize'
        AND f8.name = 'RLE_GZIP_file_size'
        JOIN
    feature f9 ON f9.col_id = cd.id
        AND f9.type = 'CompressEncFileSize'
        AND f9.name = 'BP_GZIP_file_size'
        JOIN
    feature f10 ON f10.col_id = cd.id
        AND f10.type = 'CompressEncFileSize'
        AND f10.name = 'DELTABP_GZIP_file_size'
        JOIN
    feature f11 ON f11.col_id = cd.id
        AND f11.type = 'CompressEncFileSize'
        AND f11.name = 'PLAIN_SNAPPY_file_size'
        JOIN
    feature f12 ON f12.col_id = cd.id
        AND f12.type = 'CompressEncFileSize'
        AND f12.name = 'DICT_SNAPPY_file_size'
        JOIN
    feature f13 ON f13.col_id = cd.id
        AND f13.type = 'CompressEncFileSize'
        AND f13.name = 'RLE_SNAPPY_file_size'
        JOIN
    feature f14 ON f14.col_id = cd.id
        AND f14.type = 'CompressEncFileSize'
        AND f14.name = 'BP_SNAPPY_file_size'
        JOIN
    feature f15 ON f15.col_id = cd.id
        AND f15.type = 'CompressEncFileSize'
        AND f15.name = 'DELTABP_SNAPPY_file_size'
        JOIN
    feature f16 ON f16.col_id = cd.id
        AND f16.type = 'CompressEncFileSize'
        AND f16.name = 'PLAIN_LZO_file_size'
        JOIN
    feature f17 ON f17.col_id = cd.id
        AND f17.type = 'CompressEncFileSize'
        AND f17.name = 'DICT_LZO_file_size'
        JOIN
    feature f18 ON f18.col_id = cd.id
        AND f18.type = 'CompressEncFileSize'
        AND f18.name = 'RLE_LZO_file_size'
        JOIN
    feature f19 ON f19.col_id = cd.id
        AND f19.type = 'CompressEncFileSize'
        AND f19.name = 'BP_LZO_file_size'
        JOIN
    feature f20 ON f20.col_id = cd.id
        AND f20.type = 'CompressEncFileSize'
        AND f20.name = 'DELTABP_LZO_file_size'
WHERE
    data_type = 'INTEGER'