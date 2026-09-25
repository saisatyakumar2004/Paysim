select item_id, money_at_risk
from {{ ref('fact_reconciliation') }}
where money_at_risk < 0 or duplicate_exposure < 0
