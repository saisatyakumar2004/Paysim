-- Grain: one gateway transaction (payment or refund, any status). Trusted rows only: everything here
-- passed the data-quality rules and was loaded to Silver.
select
    t.txn_id,
    t.order_id,
    m.merchant_sk,
    t.merchant_id,
    cast(t.created_at as date) as date_key,
    t.created_at,
    t.txn_type,
    t.status,
    t.method,
    t.amount,
    t.refund_of
from {{ source('silver', 'gateway_txns') }} t
left join {{ ref('dim_merchant') }} m
  on m.merchant_id = t.merchant_id
 and cast(t.created_at as date) >= m.valid_from
 and cast(t.created_at as date) < m.valid_to
