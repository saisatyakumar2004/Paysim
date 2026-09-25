-- Captured gateway movements that should be visible in a bank settlement.
-- Refunds are expected to settle as negative money.
select
    txn_id, order_id, merchant_id, txn_type, refund_of, created_at,
    cast(created_at as date) as txn_date,
    case when txn_type = 'refund' then -amount else amount end as expected_gross,
    amount
from {{ source('silver', 'gateway_txns') }}
where status = 'captured'
