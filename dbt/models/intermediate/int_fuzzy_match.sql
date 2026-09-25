-- Pass 2: fuzzy match for whatever pass 1 left over. A pair is linked only when
--   * same merchant, amount within tolerance, settled inside the SLA window,
--   * the references differ by at most N characters (typos in the bank reference),
--   * and the pair is UNIQUE from both sides (one candidate txn per settlement and vice versa).
-- Ambiguity is never guessed: those items stay unmatched and surface as exceptions.
with open_txns as (
    select * from {{ ref('int_txns_captured') }}
    where txn_id not in (select txn_id from {{ ref('int_exact_match') }})
),
open_settlements as (
    select * from {{ source('silver', 'settlements') }}
    where settlement_id not in (select settlement_id from {{ ref('int_exact_match') }})
      and gateway_ref_norm not in (select natural_key from {{ source('quarantine', 'rows') }}
                                   where dataset = 'gateway' and natural_key is not null)
),
candidates as (
    select t.txn_id, s.settlement_id,
           levenshtein(s.gateway_ref_norm, t.txn_id) as edit_distance
    from open_txns t
    join open_settlements s
      on s.merchant_id = t.merchant_id
     and abs(s.gross_amount - t.expected_gross) <= {{ var('amount_tolerance') }}
     and s.settlement_date between t.txn_date and t.txn_date + {{ var('sla_days') }}
    where levenshtein(s.gateway_ref_norm, t.txn_id) <= {{ var('max_edit_distance') }}
),
scored as (
    select *,
           count(*) over (partition by txn_id) as n_for_txn,
           count(*) over (partition by settlement_id) as n_for_settlement
    from candidates
)
select txn_id, settlement_id, 'fuzzy' as match_method, edit_distance
from scored
where n_for_txn = 1 and n_for_settlement = 1
