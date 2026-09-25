-- How long does the bank take to settle? Lag = settlement date - gateway capture date.
select
    t.txn_date,
    count(*) as settled_txns,
    round(avg(s.settlement_date - t.txn_date), 3) as avg_lag_days,
    quantile_cont(s.settlement_date - t.txn_date, 0.5) as p50_lag_days,
    quantile_cont(s.settlement_date - t.txn_date, 0.95) as p95_lag_days
from {{ ref('int_txn_settlement_links') }} l
join {{ ref('int_txns_captured') }} t on t.txn_id = l.txn_id
join {{ source('silver', 'settlements') }} s on s.settlement_id = l.settlement_id
where t.txn_type = 'payment'
group by t.txn_date
order by t.txn_date
