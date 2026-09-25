-- How long does the bank take to settle? Lag = settlement date - gateway capture date.
select
    cast(t.created_at as date) as txn_date,
    count(*) as settled_txns,
    round(avg(s.settlement_date - cast(t.created_at as date)), 3) as avg_lag_days,
    quantile_cont(s.settlement_date - cast(t.created_at as date), 0.5) as p50_lag_days,
    quantile_cont(s.settlement_date - cast(t.created_at as date), 0.95) as p95_lag_days
from {{ source('recon', 'recon_links') }} l
join {{ source('silver', 'gateway_txns') }} t on t.txn_id = l.txn_id and t.status = 'captured'
join {{ source('silver', 'settlements') }} s on s.settlement_id = l.settlement_id
where t.txn_type = 'payment'
group by cast(t.created_at as date)
order by 1
