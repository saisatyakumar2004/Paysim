-- Pass 1: exact match on the normalised bank reference.
select t.txn_id, s.settlement_id, 'exact' as match_method, 0 as edit_distance
from {{ ref('int_txns_captured') }} t
join {{ source('silver', 'settlements') }} s on s.gateway_ref_norm = t.txn_id
