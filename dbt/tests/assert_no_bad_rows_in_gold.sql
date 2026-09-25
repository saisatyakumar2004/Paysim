-- The quarantine guarantee: no transaction that was quarantined for a row-level defect may exist in Gold.
-- (Duplicate-webhook copies share the key of the kept original, so GW-UNQ-01-only rows are excluded.)
select q.natural_key
from {{ source('quarantine', 'rows') }} q
join {{ ref('fact_transactions') }} f on f.txn_id = q.natural_key
where q.dataset = 'gateway' and q.rule_ids <> 'GW-UNQ-01'
