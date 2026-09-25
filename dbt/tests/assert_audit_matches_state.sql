-- The audit trail must explain the current state: for every item, the LAST audit row's new_status equals its status
-- (items that were closed have no state row and must end on CLOSED).
with last_audit as (
    select item_id, arg_max(new_status, audit_id) as last_status
    from {{ source('recon', 'reconciliation_audit') }}
    group by item_id
)
select coalesce(a.item_id, f.item_id) as item_id, a.last_status, f.status
from last_audit a
full outer join {{ source('recon', 'fact_reconciliation') }} f on f.item_id = a.item_id
where (f.item_id is null and a.last_status <> 'CLOSED')
   or (f.item_id is not null and a.last_status is distinct from f.status)
