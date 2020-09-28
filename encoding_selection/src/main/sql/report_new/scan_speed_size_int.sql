SELECT size.value size,
       bp.value bp,
       bpsz.value bpsz,
       rle.value rle,
       rlesz.value rlesz,
       dict.value dict,
       dictsz.value dictsz,
       deltabp.value deltabp,
       deltabpsz.value deltabpsz,
       bpg.value bpg,
       bpgsz.value bpgsz,
       rleg.value rleg,
       rlegsz.value rlegsz,
       dictg.value dictg,
       dictgsz.value dictgsz,
       deltabpg.value deltabpg,
       deltabpgsz.value deltabpgsz,
       bps.value bps,
       bpssz.value bpssz,
       rles.value rles,
       rlessz.value rlessz,
       dicts.value dicts,
       dictssz.value dictssz,
       deltabps.value deltabps,
       deltabpssz.value deltabpssz
FROM col_data cd
       JOIN
     feature size ON size.col_id = cd.id
       AND size.type = 'EncFileSize'
       AND size.name = 'PLAIN_file_size'
       JOIN
     feature bp ON bp.col_id = cd.id
       AND bp.type = 'ScanTimeUsage'
       AND bp.name = 'BP_wallclock'
       JOIN
     feature bpsz on bpsz.col_id = cd.id
       AND bpsz.type = 'EncFileSize'
       AND bpsz.name = 'BP_file_size'
       JOIN
     feature rle ON rle.col_id = cd.id
       AND rle.type = 'ScanTimeUsage'
       AND rle.name = 'RLE_wallclock'
       JOIN
     feature rlesz on rlesz.col_id = cd.id
       AND rlesz.type = 'EncFileSize'
       AND rlesz.name = 'RLE_file_size'
       JOIN
     feature dict ON dict.col_id = cd.id
       AND dict.type = 'ScanTimeUsage'
       AND dict.name = 'DICT_wallclock'
       JOIN
     feature dictsz ON dictsz.col_id = cd.id
       AND dictsz.type = 'EncFileSize'
       AND dictsz.name = 'DICT_file_size'
       JOIN
     feature deltabp ON deltabp.col_id = cd.id
       AND deltabp.type = 'ScanTimeUsage'
       AND deltabp.name = 'DELTABP_wallclock'
       JOIN
     feature deltabpsz on deltabpsz.col_id = cd.id
       AND deltabpsz.type = 'EncFileSize'
       AND deltabpsz.name = 'DELTABP_file_size'
       JOIN
     feature bpg ON bpg.col_id = cd.id
       AND bpg.type = 'ScanTimeUsage'
       AND bpg.name = 'BP_GZIP_wallclock'
       JOIN
     feature bpgsz on bpgsz.col_id = cd.id
       AND bpgsz.type = 'CompressEncFileSize'
       AND bpgsz.name = 'BP_GZIP_file_size'
       JOIN
     feature rleg ON rleg.col_id = cd.id
       AND rleg.type = 'ScanTimeUsage'
       AND rleg.name = 'RLE_GZIP_wallclock'
       JOIN
     feature rlegsz on rlegsz.col_id = cd.id
       AND rlegsz.type = 'CompressEncFileSize'
       AND rlegsz.name = 'RLE_GZIP_file_size'
       JOIN
     feature dictg ON dictg.col_id = cd.id
       AND dictg.type = 'ScanTimeUsage'
       AND dictg.name = 'DICT_GZIP_wallclock'
       JOIN
     feature dictgsz on dictgsz.col_id = cd.id
       AND dictgsz.type = 'CompressEncFileSize'
       AND dictgsz.name = 'DICT_GZIP_file_size'
       JOIN
     feature deltabpg ON deltabpg.col_id = cd.id
       AND deltabpg.type = 'ScanTimeUsage'
       AND deltabpg.name = 'DELTABP_GZIP_wallclock'
       JOIN
     feature deltabpgsz on deltabpgsz.col_id = cd.id
       AND deltabpgsz.type = 'CompressEncFileSize'
       AND deltabpgsz.name = 'DELTABP_GZIP_file_size'
       JOIN
     feature bps ON bps.col_id = cd.id
       AND bps.type = 'ScanTimeUsage'
       AND bps.name = 'BP_LZO_wallclock'
       JOIN
     feature bpssz on bpssz.col_id = cd.id
       AND bpssz.type = 'CompressEncFileSize'
       AND bpssz.name = 'BP_LZO_file_size'
       JOIN
     feature rles ON rles.col_id = cd.id
       AND rles.type = 'ScanTimeUsage'
       AND rles.name = 'RLE_LZO_wallclock'
       JOIN
     feature rlessz on rlessz.col_id = cd.id
       AND rlessz.type = 'CompressEncFileSize'
       AND rlessz.name = 'RLE_LZO_file_size'
       JOIN
     feature dicts ON dicts.col_id = cd.id
       AND dicts.type = 'ScanTimeUsage'
       AND dicts.name = 'DICT_LZO_wallclock'
       JOIN
     feature dictssz on dictssz.col_id = cd.id
       AND dictssz.type = 'CompressEncFileSize'
       AND dictssz.name = 'DICT_LZO_file_size'
       JOIN
     feature deltabps ON deltabps.col_id = cd.id
       AND deltabps.type = 'ScanTimeUsage'
       AND deltabps.name = 'DELTABP_LZO_wallclock'
       JOIN
     feature deltabpssz on deltabpssz.col_id = cd.id
       AND deltabpssz.type = 'CompressEncFileSize'
       AND deltabpssz.name = 'DELTABP_LZO_file_size'
WHERE cd.data_type = 'INTEGER'
  AND size.value > 5000000
  AND cd.parent_id is NULL
  AND cd.id < 15016