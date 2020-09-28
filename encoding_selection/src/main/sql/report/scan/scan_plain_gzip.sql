select 
    file_size.value fs,
    scan_time.value wc,
    scan_cpu.value cput,
    scan_user.value ut
from
    col_data cd
        join
    feature file_size ON file_size.col_id = cd.id
        and file_size.type = 'CompressEncFileSize'
        and file_size.name = 'BP_LZO_file_size'
        join
    feature scan_time ON scan_time.col_id = cd.id
        and scan_time.type = 'ScanTimeUsage'
        and scan_time.name = 'BP_LZO_wallclock'
        join
    feature scan_cpu ON scan_cpu.col_id = cd.id
        and scan_cpu.type = 'ScanTimeUsage'
        and scan_cpu.name = 'BP_LZO_cpu'
        join
    feature scan_user ON scan_user.col_id = cd.id
        and scan_user.type = 'ScanTimeUsage'
        and scan_user.name = 'BP_LZO_user'
where cd.data_type = 'INTEGER'
limit 20000