-- Grain: one bank settlement line.
select
    s.settlement_id,
    s.gateway_ref_norm,
    l.txn_id,
    l.match_method,
    m.merchant_sk,
    s.merchant_id,
    s.settlement_date as date_key,
    s.gross_amount,
    s.fee_amount,
    s.net_amount,
    s.net_consistent
from {{ source('silver', 'settlements') }} s
left join {{ ref('int_txn_settlement_links') }} l on l.settlement_id = s.settlement_id
left join {{ ref('dim_merchant') }} m
  on m.merchant_id = s.merchant_id
 and s.settlement_date >= m.valid_from
 and s.settlement_date < m.valid_to
