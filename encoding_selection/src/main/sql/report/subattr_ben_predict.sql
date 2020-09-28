select f.value,lm.value,lv.value,em.value,ev.value,c.value from col_data cd join
feature f on f.col_id = cd.id
and f.type = 'Distinct' and f.name='ratio'
join
feature lm on lm.col_id = cd.id
and lm.type = 'Length' and lm.name='mean'
join
feature lv on lv.col_id = cd.id
and lv.type = 'Length' and lv.name='variance'
join
feature em on em.col_id = cd.id
and em.type = 'Entropy' and em.name='line_mean'
join
feature ev on ev.col_id = cd.id
and ev.type = 'Entropy' and ev.name='line_var'
join col_info c on c.col_id = cd.id and c.name = 'subattr_benefit'

limit 50000