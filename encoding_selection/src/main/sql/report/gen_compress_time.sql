select 
    file_size.value fs,
    enc_time.value wc,
    enc_cpu.value cput,
    enc_user.value ut
from
    col_data cd
        join
    feature file_size ON file_size.col_id = cd.id
        and file_size.type = 'EncFileSize'
        and file_size.name = 'PLAIN_file_size'
        join
    feature enc_time ON enc_time.col_id = cd.id
        and enc_time.type = 'CompressTimeUsage'
        and enc_time.name = 'PLAIN_LZO_wctime'
        join
    feature enc_cpu ON enc_cpu.col_id = cd.id
        and enc_cpu.type = 'CompressTimeUsage'
        and enc_cpu.name = 'PLAIN_LZO_cputime'
        join
    feature enc_user ON enc_user.col_id = cd.id
        and enc_user.type = 'CompressTimeUsage'
        and enc_user.name = 'PLAIN_LZO_usertime'
where cd.data_type = 'INTEGER'
limit 20000