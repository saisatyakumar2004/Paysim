-- Which merchants create reconciliation work? Uses the CURRENT merchant attributes.
with by_merchant as (
    select
        merchant_id,
        count(*) filter (where item_type = 'txn') as txns,
        count(*) filter (where status in ('AMOUNT_MISMATCH', 'MISSING_SETTLEMENT', 'ORPHAN_REFUND')) as exceptions,
        count(*) filter (where status = 'AMOUNT_MISMATCH') as amount_mismatch,
        count(*) filter (where status = 'MISSING_SETTLEMENT') as missing_settlement,
        count(*) filter (where is_duplicate_charge) as duplicate_charges,
        sum(money_at_risk) + sum(duplicate_exposure) as total_exposure
    from {{ source('recon', 'fact_reconciliation') }}
    where merchant_id is not null
    group by merchant_id
)
select
    b.*,
    round(b.exceptions * 1.0 / nullif(b.txns, 0), 4) as exception_rate,
    rank() over (order by b.total_exposure desc) as exposure_rank,
    m.name, m.category, m.city, m.risk_tier
from by_merchant b
left join {{ ref('dim_merchant') }} m on m.merchant_id = b.merchant_id and m.is_current
