-- MATCHED must mean matched: no item may be MATCHED with a difference above the tolerance.
select item_id, diff_amount
from {{ source('recon', 'fact_reconciliation') }}
where status = 'MATCHED' and abs(diff_amount) > {{ var('amount_tolerance') }}
