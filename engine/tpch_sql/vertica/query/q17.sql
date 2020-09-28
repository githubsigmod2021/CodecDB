select sum(l_extendedprice) / 7.0 as avg_yearly
from lineitem,
     part
where p_partkey = l_partkey
  and p_brand = 'Brand#23'
  and p_container = 'MED BOX'
  and l_quantity < (
    select 0.2 * avg(l_quantity)
    from lineitem
    where l_partkey = p_partkey
);

SELECT last_statement_duration_us / 1000000.0 last_statement_duration_seconds
FROM current_session;