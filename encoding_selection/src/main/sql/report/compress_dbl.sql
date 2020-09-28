select
    plain.value AS plain,
	dict.value as dict,
    plaingz.value as gz,
    plainlz.value as lz,
    plainsn.value as sn
from
    col_data cd
        join
    feature plain ON plain.col_id = cd.id
        and plain.type = 'EncFileSize'
        and plain.name = 'PLAIN_file_size'
        join
    feature dict ON dict.col_id = cd.id
        and dict.type = 'EncFileSize'
        and dict.name = 'DICT_file_size'
        join
    feature plaingz ON plaingz.col_id = cd.id
        and plaingz.type = 'CompressEncFileSize'
        and plaingz.name = 'PLAIN_GZIP_file_size'
        join
    feature plainlz ON plainlz.col_id = cd.id
        and plainlz.type = 'CompressEncFileSize'
        and plainlz.name = 'PLAIN_LZO_file_size'
        join
    feature plainsn ON plainsn.col_id = cd.id
        and plainsn.type = 'CompressEncFileSize'
        and plainsn.name = 'PLAIN_SNAPPY_file_size'
where
    cd.data_type = 'DOUBLE'
        and cd.parent_id is NULL limit 50000