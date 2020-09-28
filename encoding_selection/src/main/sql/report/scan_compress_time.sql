select 
    plain.value plain,
    file_size.value fs,
	gen_time.value gen_wc,
	gen_cpu.value gen_cpu,
    scan_time.value scan_wc,
    scan_cpu.value scan_cpu
from
    col_data cd
        join
    feature plain ON plain.col_id = cd.id
        and plain.type = 'EncFileSize'
        and plain.name = 'PLAIN_file_size'
        join
    feature file_size ON file_size.col_id = cd.id
        and file_size.type = 'CompressEncFileSize'
        and file_size.name = 'PLAIN_LZO_file_size'
        join
    feature gen_time ON gen_time.col_id = cd.id
        and gen_time.type = 'CompressTimeUsage'
        and gen_time.name = 'PLAIN_LZO_wctime'
        join
    feature gen_cpu ON gen_cpu.col_id = cd.id
        and gen_cpu.type = 'CompressTimeUsage'
        and gen_cpu.name = 'PLAIN_LZO_cputime'
        join
    feature scan_time ON scan_time.col_id = cd.id
        and scan_time.type = 'ScanTimeUsage'
        and scan_time.name = 'PLAIN_LZO_wallclock'
        join
    feature scan_cpu ON scan_cpu.col_id = cd.id
        and scan_cpu.type = 'ScanTimeUsage'
        and scan_cpu.name = 'PLAIN_LZO_cpu'
where
    cd.data_type = 'STRING'
limit 20000