SELECT 
	cd.id,
    f1.value AS 'Sparsity_valid_ratio',
    f2.value AS 'Entropy_line_max',
    f3.value AS 'Entropy_line_min',
    f4.value AS 'Entropy_line_mean',
    f5.value AS 'Entropy_line_var',
    f6.value AS 'Length_max',
    f7.value AS 'Length_min',
    f8.value AS 'Length_mean',
    f9.value AS 'Length_var',
    f10.value AS 'Distinct_ratio',
    f11.value AS 'Sortness_ivpair_50',
    f12.value AS 'Sortness_kendalltau_50',
    f13.value AS 'Sortness_spearmanrho_50',
    f14.value AS 'Sortness_ivpair_100',
    f15.value AS 'Sortness_kendalltau_100',
    f16.value AS 'Sortness_spearmanrho_100',
    f17.value AS 'Sortness_ivpair_200',
    f18.value AS 'Sortness_kendalltau_200',
    f19.value AS 'Sortness_spearmanrho_200',
    (CASE mine.name
        WHEN 'PLAIN_file_size' THEN 0
        WHEN 'DICT_file_size' THEN 1
        WHEN 'BP_file_size' THEN 2
        WHEN 'RLE_file_size' THEN 3
        WHEN 'DELTABP_file_size' THEN 4
        ELSE 0
    END) AS 'Encoding'
FROM
    col_data cd
        JOIN
    feature f1 ON f1.col_id = cd.id
        AND f1.type = 'Sparsity'
        AND f1.name = 'valid_ratio'
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
    feature f6 ON f6.col_id = cd.id AND f6.type = 'Length'
        AND f6.name = 'max'
        JOIN
    feature f7 ON f7.col_id = cd.id AND f7.type = 'Length'
        AND f7.name = 'min'
        JOIN
    feature f8 ON f8.col_id = cd.id AND f8.type = 'Length'
        AND f8.name = 'mean'
        JOIN
    feature f9 ON f9.col_id = cd.id AND f9.type = 'Length'
        AND f9.name = 'variance'
        JOIN
    feature f10 ON f10.col_id = cd.id
        AND f10.type = 'Distinct'
        AND f10.name = 'ratio'
        JOIN
    feature f11 ON f11.col_id = cd.id
        AND f11.type = 'Sortness'
        AND f11.name = 'ivpair_50'
        JOIN
    feature f12 ON f12.col_id = cd.id
        AND f12.type = 'Sortness'
        AND f12.name = 'kendalltau_50'
        JOIN
    feature f13 ON f13.col_id = cd.id
        AND f13.type = 'Sortness'
        AND f13.name = 'spearmanrho_50'
        JOIN
    feature f14 ON f14.col_id = cd.id
        AND f14.type = 'Sortness'
        AND f14.name = 'ivpair_100'
        JOIN
    feature f15 ON f15.col_id = cd.id
        AND f15.type = 'Sortness'
        AND f15.name = 'kendalltau_100'
        JOIN
    feature f16 ON f16.col_id = cd.id
        AND f16.type = 'Sortness'
        AND f16.name = 'spearmanrho_100'
        JOIN
    feature f17 ON f17.col_id = cd.id
        AND f17.type = 'Sortness'
        AND f17.name = 'ivpair_200'
        JOIN
    feature f18 ON f18.col_id = cd.id
        AND f18.type = 'Sortness'
        AND f18.name = 'kendalltau_200'
        JOIN
    feature f19 ON f19.col_id = cd.id
        AND f19.type = 'Sortness'
        AND f19.name = 'spearmanrho_200'
        JOIN
    (SELECT 
        minenc.col_id, minenc.name
    FROM
        feature minenc
    JOIN (SELECT 
        col_id, MIN(value) value
    FROM
        feature
    WHERE
        type = 'EncFileSize' AND value >= 0
    GROUP BY col_id) minval ON minenc.col_id = minval.col_id
        AND minenc.value = minval.value
    WHERE
        minenc.type = 'EncFileSize') mine ON mine.col_id = cd.id
WHERE
    cd.data_type = 'INTEGER' and 'Encoding' <> 5
limit 60000