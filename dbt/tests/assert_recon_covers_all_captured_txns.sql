-- Every captured Silver transaction must appear exactly once as a reconciliation item.
select t.txn_id
from {{ source('silver', 'gateway_txns') }} t
left join {{ ref('fact_reconciliation') }} r on r.txn_id = t.txn_id and r.item_type = 'txn'
where t.status = 'captured'
group by t.txn_id
having count(r.item_id) <> 1
