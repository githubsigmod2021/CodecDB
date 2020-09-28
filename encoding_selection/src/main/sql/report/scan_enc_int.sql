select 
    plains.value,
    plain.value,
    bps.value,
    bp.value,
    rles.value,
    rle.value,
    deltabp.value,
    delta.value,
    dicts.value,
    dict.value
from
    col_data cd
        join
    feature plains ON plains.col_id = cd.id
        and plains.type = 'EncFileSize'
        and plains.name = 'PLAIN_file_size'
        join
    feature plain ON plain.col_id = cd.id
        and plain.type = 'ScanTimeUsage'
        and plain.name = 'PLAIN_cpu'
        join
    feature bps ON bps.col_id = cd.id
        and bps.type = 'EncFileSize'
        and bps.name = 'BP_file_size'
        join
    feature bp ON bp.col_id = cd.id
        and bp.type = 'ScanTimeUsage'
        and bp.name = 'BP_cpu'
        join
    feature rles ON rles.col_id = cd.id
        and rles.type = 'EncFileSize'
        and rles.name = 'RLE_file_size'
        join
    feature rle ON rle.col_id = cd.id
        and rle.type = 'ScanTimeUsage'
        and rle.name = 'RLE_cpu'
        join
    feature dbps ON dbps.col_id = cd.id
        and dbps.type = 'EncFileSize'
        and dbps.name = 'DELTABP_file_size'
        join
    feature deltabp ON deltabp.col_id = cd.id
        and deltabp.type = 'ScanTimeUsage'
        and deltabp.name = 'DELTABP_cpu'
        join
    feature dicts ON dicts.col_id = cd.id
        and dicts.type = 'EncFileSize'
        and dicts.name = 'DICT_file_size'
        join
    feature dict ON dict.col_id = cd.id
        and dict.type = 'ScanTimeUsage'
        and dict.name = 'DICT_cpu'
limit 10000