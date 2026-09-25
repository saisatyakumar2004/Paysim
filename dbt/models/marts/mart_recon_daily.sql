-- Daily reconciliation scorecard (executive view). match_rate excludes PENDING items (settlement not yet due).
select
    event_date,
    count(*) filter (where item_type = 'txn') as txns,
    count(*) filter (where status = 'MATCHED') as matched,
    count(*) filter (where status = 'AMOUNT_MISMATCH') as amount_mismatch,
    count(*) filter (where status = 'MISSING_SETTLEMENT') as missing_settlement,
    count(*) filter (where status = 'PENDING') as pending,
    count(*) filter (where status = 'ORPHAN_REFUND') as orphan_refund,
    count(*) filter (where status = 'ORPHAN_SETTLEMENT') as orphan_settlement,
    count(*) filter (where status = 'ORDER_WITHOUT_PAYMENT') as order_without_payment,
    count(*) filter (where is_duplicate_charge) as duplicate_charges,
    count(*) filter (where match_method = 'fuzzy') as fuzzy_matched,
    round(count(*) filter (where status = 'MATCHED') * 1.0 /
          nullif(count(*) filter (where item_type = 'txn' and status not in ('PENDING', 'ORPHAN_REFUND')), 0), 4) as match_rate,
    round(sum(money_at_risk), 2) as money_at_risk,
    round(sum(duplicate_exposure), 2) as duplicate_exposure
from {{ ref('fact_reconciliation') }}
group by event_date
order by event_date
