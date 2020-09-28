select 
    fs.value,
    gentime.value,
    gencpu.value,
    scantime.value,
    scancpu.value
from
    col_data cd
        join
    feature fs ON fs.col_id = cd.id
        and fs.type = 'EncFileSize'
        and fs.name = 'DICT_file_size'
        join
    feature gentime ON gentime.col_id = cd.id
        and gentime.type = 'EncTimeUsage'
        and gentime.name = 'DICT_wctime'
        join
    feature gencpu ON gencpu.col_id = cd.id
        and gencpu.type = 'EncTimeUsage'
        and gencpu.name = 'DICT_cputime'
        join
    feature scantime ON scantime.col_id = cd.id
        and scantime.type = 'ScanTimeUsage'
        and scantime.name = 'DICT_wallclock'
        join
    feature scancpu ON scancpu.col_id = cd.id
        and scancpu.type = 'ScanTimeUsage'
        and scancpu.name = 'DICT_cpu'
where
    cd.data_type = 'STRING'