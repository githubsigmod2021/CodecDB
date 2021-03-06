SELECT 
    f1.value,
    f2.value,
    f3.value,
    f4.value,
    f5.value,
    f6.value,
    f7.value,
    f8.value,
    f9.value,
    f10.value,
    f11.value,
    f12.value,
    f13.value,
    f14.value,
    f15.value,
    f16.value,
    f17.value,
    f18.value,
    f19.value,
    f20.value,
    r1,
    r2,
    r3,
    r4,
    r5,
    r6,
    r7,
    r8,
    r9,
    r10,
    r11,
    r12,
    r13,
    r14,
    r15,
    r16,
    r17,
    r18,
    r19
FROM
    col_data cd
        JOIN
    feature f1 ON f1.col_id = cd.id
        AND f1.type = 'Distinct'
        AND f1.name = 'ratio'
        JOIN
    feature f2 ON f2.col_id = cd.id
        AND f2.type = 'Entropy'
        AND f2.name = 'line_max'
        JOIN
    feature f3 ON f3.col_id = cd.id
        AND f3.type = 'Entropy'
        AND f3.name = 'line_min'
        JOIN
    feature f4 ON f4.col_id = cd.id
        AND f4.type = 'Entropy'
        AND f4.name = 'line_mean'
        JOIN
    feature f5 ON f5.col_id = cd.id
        AND f5.type = 'Entropy'
        AND f5.name = 'line_var'
        JOIN
    feature f6 ON f6.col_id = cd.id
        AND f6.type = 'Entropy'
        AND f6.name = 'total'
        JOIN
    feature f7 ON f7.col_id = cd.id AND f7.type = 'Length'
        AND f7.name = 'max'
        JOIN
    feature f8 ON f8.col_id = cd.id AND f8.type = 'Length'
        AND f8.name = 'min'
        JOIN
    feature f9 ON f9.col_id = cd.id AND f9.type = 'Length'
        AND f9.name = 'mean'
        JOIN
    feature f10 ON f10.col_id = cd.id
        AND f10.type = 'Length'
        AND f10.name = 'variance'
        JOIN
    feature f11 ON f11.col_id = cd.id
        AND f11.type = 'Sparsity'
        AND f11.name = 'valid_ratio'
        JOIN
    feature f12 ON f12.col_id = cd.id
        AND f12.type = 'Sortness'
        AND f12.name = 'ivpair_50'
        JOIN
    feature f13 ON f13.col_id = cd.id
        AND f13.type = 'Sortness'
        AND f13.name = 'ivpair_100'
        JOIN
    feature f14 ON f14.col_id = cd.id
        AND f14.type = 'Sortness'
        AND f14.name = 'ivpair_200'
        JOIN
    feature f15 ON f15.col_id = cd.id
        AND f15.type = 'Sortness'
        AND f15.name = 'kendalltau_50'
        JOIN
    feature f16 ON f16.col_id = cd.id
        AND f16.type = 'Sortness'
        AND f16.name = 'kendalltau_100'
        JOIN
    feature f17 ON f17.col_id = cd.id
        AND f17.type = 'Sortness'
        AND f17.name = 'kendalltau_200'
        JOIN
    feature f18 ON f18.col_id = cd.id
        AND f18.type = 'Sortness'
        AND f18.name = 'spearmanrho_50'
        JOIN
    feature f19 ON f19.col_id = cd.id
        AND f19.type = 'Sortness'
        AND f19.name = 'spearmanrho_100'
        JOIN
    feature f20 ON f20.col_id = cd.id
        AND f20.type = 'Sortness'
        AND f20.name = 'spearmanrho_200'
        JOIN
    (SELECT 
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
    JOIN feature f1 ON f1.col_id = cd.id
        AND f1.type = 'EncFileSize'
        AND f1.name = 'PLAIN_file_size'
    JOIN feature f2 ON f2.col_id = cd.id
        AND f2.type = 'EncFileSize'
        AND f2.name = 'DICT_file_size'
    JOIN feature f3 ON f3.col_id = cd.id
        AND f3.type = 'EncFileSize'
        AND f3.name = 'RLE_file_size'
    JOIN feature f4 ON f4.col_id = cd.id
        AND f4.type = 'EncFileSize'
        AND f4.name = 'BP_file_size'
    JOIN feature f5 ON f5.col_id = cd.id
        AND f5.type = 'EncFileSize'
        AND f5.name = 'DELTABP_file_size'
    JOIN feature f6 ON f6.col_id = cd.id
        AND f6.type = 'CompressEncFileSize'
        AND f6.name = 'PLAIN_GZIP_file_size'
    JOIN feature f7 ON f7.col_id = cd.id
        AND f7.type = 'CompressEncFileSize'
        AND f7.name = 'DICT_GZIP_file_size'
    JOIN feature f8 ON f8.col_id = cd.id
        AND f8.type = 'CompressEncFileSize'
        AND f8.name = 'RLE_GZIP_file_size'
    JOIN feature f9 ON f9.col_id = cd.id
        AND f9.type = 'CompressEncFileSize'
        AND f9.name = 'BP_GZIP_file_size'
    JOIN feature f10 ON f10.col_id = cd.id
        AND f10.type = 'CompressEncFileSize'
        AND f10.name = 'DELTABP_GZIP_file_size'
    JOIN feature f11 ON f11.col_id = cd.id
        AND f11.type = 'CompressEncFileSize'
        AND f11.name = 'PLAIN_SNAPPY_file_size'
    JOIN feature f12 ON f12.col_id = cd.id
        AND f12.type = 'CompressEncFileSize'
        AND f12.name = 'DICT_SNAPPY_file_size'
    JOIN feature f13 ON f13.col_id = cd.id
        AND f13.type = 'CompressEncFileSize'
        AND f13.name = 'RLE_SNAPPY_file_size'
    JOIN feature f14 ON f14.col_id = cd.id
        AND f14.type = 'CompressEncFileSize'
        AND f14.name = 'BP_SNAPPY_file_size'
    JOIN feature f15 ON f15.col_id = cd.id
        AND f15.type = 'CompressEncFileSize'
        AND f15.name = 'DELTABP_SNAPPY_file_size'
    JOIN feature f16 ON f16.col_id = cd.id
        AND f16.type = 'CompressEncFileSize'
        AND f16.name = 'PLAIN_LZO_file_size'
    JOIN feature f17 ON f17.col_id = cd.id
        AND f17.type = 'CompressEncFileSize'
        AND f17.name = 'DICT_LZO_file_size'
    JOIN feature f18 ON f18.col_id = cd.id
        AND f18.type = 'CompressEncFileSize'
        AND f18.name = 'RLE_LZO_file_size'
    JOIN feature f19 ON f19.col_id = cd.id
        AND f19.type = 'CompressEncFileSize'
        AND f19.name = 'BP_LZO_file_size'
    JOIN feature f20 ON f20.col_id = cd.id
        AND f20.type = 'CompressEncFileSize'
        AND f20.name = 'DELTABP_LZO_file_size'
    WHERE
        cd.data_type = 'INTEGER') ratio ON ratio.id = cd.id
WHERE
    cd.data_type = 'INTEGER'
LIMIT 50000