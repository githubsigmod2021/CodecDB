select 
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
    (case label.name
        when 'PLAIN_file_size' then 0
        when 'DICT_file_size' then 1
        when 'DELTA_file_size' then 2
        when 'DELTAL_file_size' then 3
    end) L
from
    col_data cd
        join
    feature f1 ON f1.col_id = cd.id
        and f1.type = 's10k_Distinct'
        and f1.name = 'ratio'
        join
    feature f2 ON f2.col_id = cd.id
        and f2.type = 's10k_Entropy'
        and f2.name = 'line_max'
        join
    feature f3 ON f3.col_id = cd.id
        and f3.type = 's10k_Entropy'
        and f3.name = 'line_min'
        join
    feature f4 ON f4.col_id = cd.id
        and f4.type = 's10k_Entropy'
        and f4.name = 'line_mean'
        join
    feature f5 ON f5.col_id = cd.id
        and f5.type = 's10k_Entropy'
        and f5.name = 'line_var'
        join
    feature f6 ON f6.col_id = cd.id
        and f6.type = 's10k_Entropy'
        and f6.name = 'total'
        join
    feature f7 ON f7.col_id = cd.id and f7.type = 's10k_Length'
        and f7.name = 'max'
        join
    feature f8 ON f8.col_id = cd.id and f8.type = 's10k_Length'
        and f8.name = 'min'
        join
    feature f9 ON f9.col_id = cd.id and f9.type = 's10k_Length'
        and f9.name = 'mean'
        join
    feature f10 ON f10.col_id = cd.id
        and f10.type = 's10k_Length'
        and f10.name = 'variance'
        join
    feature f11 ON f11.col_id = cd.id
        and f11.type = 's10k_Sparsity'
        and f11.name = 'valid_ratio'
        join
    feature f12 ON f12.col_id = cd.id
        and f12.type = 's10k_Sortness'
        and f12.name = 'ivpair_50'
        join
    feature f13 ON f13.col_id = cd.id
        and f13.type = 's10k_Sortness'
        and f13.name = 'ivpair_100'
        join
    feature f14 ON f14.col_id = cd.id
        and f14.type = 's10k_Sortness'
        and f14.name = 'ivpair_200'
        join
    feature f15 ON f15.col_id = cd.id
        and f15.type = 's10k_Sortness'
        and f15.name = 'kendalltau_50'
        join
    feature f16 ON f16.col_id = cd.id
        and f16.type = 's10k_Sortness'
        and f16.name = 'kendalltau_100'
        join
    feature f17 ON f17.col_id = cd.id
        and f17.type = 's10k_Sortness'
        and f17.name = 'kendalltau_200'
        join
    feature f18 ON f18.col_id = cd.id
        and f18.type = 's10k_Sortness'
        and f18.name = 'spearmanrho_50'
        join
    feature f19 ON f19.col_id = cd.id
        and f19.type = 's10k_Sortness'
        and f19.name = 'spearmanrho_100'
        join
    feature f20 ON f20.col_id = cd.id
        and f20.type = 's10k_Sortness'
        and f20.name = 'spearmanrho_200'
        join
    feature label ON label.col_id = cd.id
        and label.type = 'EncFileSize'
        and label.value = (select 
            min(value)
        from
            feature enc
        where
            enc.col_id = cd.id
                and enc.type = 'EncFileSize'
                and enc.name <> 'BITVECTOR_file_size'
                and enc.value > 0)
where
    cd.data_type = 'STRING'
limit 50000