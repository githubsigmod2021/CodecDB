SELECT 
    plains.value,
    plain.value,
    dict.value,
    bp.value,
    rle.value,
    deltabp.value
FROM
    col_data cd
        JOIN
    feature plains ON plains.col_id = cd.id
        AND plains.type = 'EncFileSize'
        AND plains.name = 'PLAIN_file_size'
        JOIN
    feature plain ON plain.col_id = cd.id
        AND plain.type = 'EncTimeUsage'
        AND plain.name = 'PLAIN_cputime'
        JOIN
    feature dict ON dict.col_id = cd.id
        AND dict.type = 'EncTimeUsage'
        AND dict.name = 'DICT_cputime'
        JOIN
    feature bp ON bp.col_id = cd.id
        AND bp.type = 'EncTimeUsage'
        AND bp.name = 'BP_cputime'
        JOIN
    feature rle ON rle.col_id = cd.id
        AND rle.type = 'EncTimeUsage'
        AND rle.name = 'RLE_cputime'
        JOIN
    feature deltabp ON deltabp.col_id = cd.id
        AND deltabp.type = 'EncTimeUsage'
        AND deltabp.name = 'DELTABP_cputime'
LIMIT 20000