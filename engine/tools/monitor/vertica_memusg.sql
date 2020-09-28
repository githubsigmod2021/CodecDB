SELECT memory_inuse_kb
FROM V_MONITOR.RESOURCE_ACQUISITIONS
where pool_name = 'general'
order by acquisition_timestamp desc
limit 1;