-- A customer charged twice for one order: same order, merchant and amount, captured again
-- within a few minutes under a different txn_id. The LATER charge is the duplicate.
select t.txn_id, t.order_id, t.amount as duplicate_exposure, p.txn_id as original_txn_id
from {{ ref('int_txns_captured') }} t
join {{ ref('int_txns_captured') }} p
  on p.order_id = t.order_id and p.merchant_id = t.merchant_id and p.amount = t.amount
 and p.txn_type = 'payment' and t.txn_type = 'payment'
 and p.txn_id <> t.txn_id
 and p.created_at < t.created_at
 and t.created_at <= p.created_at + interval '{{ var("dup_window_minutes") }} minutes'
