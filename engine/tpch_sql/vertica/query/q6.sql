select
    sum(l_extendedprice*l_discount) as revenue
from
    lineitem
where
        l_shipdate >= '1994-01-01'
  and l_shipdate < '1995-01-01'
  and l_discount between 0.04 and 0.06
  and l_quantity < 24;

SELECT last_statement_duration_us / 1000000.0 last_statement_duration_seconds
FROM current_session;