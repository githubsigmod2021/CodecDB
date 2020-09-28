select
    nation,
    o_year,
    sum(amount) as sum_profit
from (
         select
             n_name as nation,
             substr(o_orderdate,1,4) as o_year,
             l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
         from
             part,
             supplier,
             lineitem,
             partsupp,
             orders,
             nation
         where
                 s_suppkey = l_suppkey
           and ps_suppkey = l_suppkey
           and ps_partkey = l_partkey
           and p_partkey = l_partkey
           and o_orderkey = l_orderkey
           and s_nationkey = n_nationkey
           and p_name like '%green%'
     ) as profit
group by
    nation,
    o_year
order by
    nation,
    o_year desc;

SELECT last_statement_duration_us / 1000000.0 last_statement_duration_seconds
FROM current_session;