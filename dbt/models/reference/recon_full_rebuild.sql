{{ config(tags=['reference']) }}
-- REFERENCE IMPLEMENTATION (full rebuild). The daily pipeline uses the incremental engine in
-- src/payreconcile/reconcile.py; this model is the independent oracle used by `verify` and the benchmark.
-- One row per reconciliation item across the three-way match (gateway <-> bank <-> orders).
--   item_type = txn         a captured gateway movement (payment or refund)
--   item_type = settlement  bank money that could not be linked to any gateway txn
--   item_type = order       a paid order with no captured payment
{% set as_of = "cast('" ~ var('as_of_date') ~ "' as date)" %}

{% set win = var('dup_window_minutes') * 60 %}
{% set qamt = "pr_amt(json_extract_string(q.raw_json, '$.amount'))" %}
{% set qts = "pr_ts(json_extract_string(q.raw_json, '$.created_at'))" %}

with gateway_keys as (
    select natural_key from {{ source('quarantine', 'rows') }}
    where dataset = 'gateway' and natural_key is not null
),
qdup as (
    select t.txn_id, min(q.natural_key) as duplicate_of
    from {{ ref('int_txns_captured') }} t
    join {{ source('quarantine', 'rows') }} q
      on q.dataset = 'gateway' and q.order_id = t.order_id and q.natural_key <> t.txn_id
    where t.txn_type = 'payment' and t.order_id is not null
      and q.natural_key not in (select txn_id from {{ source('silver', 'gateway_txns') }})
      and coalesce(lower(json_extract_string(q.raw_json, '$.txn_type')), 'payment') = 'payment'
      and coalesce(lower(json_extract_string(q.raw_json, '$.status')), '') not in ('failed', 'pending')
      and ({{ qamt }} is null or {{ qamt }} <= 0 or {{ qamt }} = t.amount)
      and ({{ qts }} is null or abs(epoch({{ qts }} - t.created_at)) > 86400 or abs(epoch({{ qts }} - t.created_at)) <= {{ win }})
    group by t.txn_id
),
linked as (
    -- one row per txn even when the bank references it more than once (split or duplicate settlement)
    select k.txn_id, min(k.settlement_id) as settlement_id, min(k.match_method) as match_method,
           sum(s.gross_amount) as gross_amount
    from {{ ref('int_txn_settlement_links') }} k
    join {{ source('silver', 'settlements') }} s on s.settlement_id = k.settlement_id
    group by k.txn_id
),
txn_items as (
    select
        'TXN:' || t.txn_id as item_id,
        'txn' as item_type,
        t.txn_id,
        l.settlement_id,
        t.order_id,
        t.merchant_id,
        t.txn_date as event_date,
        t.expected_gross as expected_amount,
        l.gross_amount as settled_amount,
        l.gross_amount - t.expected_gross as diff_amount,
        case
            when t.txn_type = 'refund'
                 and t.refund_of not in (select txn_id from {{ source('silver', 'gateway_txns') }})
                 and t.refund_of not in (select natural_key from gateway_keys)
                then 'ORPHAN_REFUND'
            when l.settlement_id is null then
                case when t.txn_date + {{ var('sla_days') }} < {{ as_of }} then 'MISSING_SETTLEMENT' else 'PENDING' end
            when abs(l.gross_amount - t.expected_gross) <= {{ var('amount_tolerance') }} then 'MATCHED'
            else 'AMOUNT_MISMATCH'
        end as status,
        l.match_method,
        d.txn_id is not null as is_duplicate_charge,
        coalesce(d.duplicate_exposure, 0) as duplicate_exposure,
        case
            when t.txn_type = 'payment' and o.order_id is null
                 and t.order_id not in (select natural_key from {{ source('quarantine', 'rows') }} where dataset = 'orders' and natural_key is not null)
                then 'NO_ORDER'
            when t.txn_type = 'payment' and o.order_id is not null
                 and abs(o.order_amount - t.amount) > {{ var('amount_tolerance') }}
                then 'AMOUNT_DIFF'
        end as order_issue,
        d.txn_id is null and qd.txn_id is not null as is_suspected_duplicate,
        coalesce(d.original_txn_id, qd.duplicate_of) as duplicate_of,
        case when d.txn_id is not null then 'TRUSTED_PAIR'
             when qd.txn_id is not null then 'QUARANTINED_ORIGINAL' end as duplicate_basis,
        case when d.txn_id is null and qd.txn_id is not null then t.amount else 0 end as suspected_duplicate_exposure
    from {{ ref('int_txns_captured') }} t
    left join linked l on l.txn_id = t.txn_id
    left join {{ ref('int_duplicate_charges') }} d on d.txn_id = t.txn_id
    left join qdup qd on qd.txn_id = t.txn_id
    left join {{ source('silver', 'orders') }} o on o.order_id = t.order_id
),
settlement_items as (
    select
        'STL:' || s.settlement_id as item_id,
        'settlement' as item_type,
        cast(null as varchar) as txn_id,
        s.settlement_id,
        cast(null as varchar) as order_id,
        s.merchant_id,
        s.settlement_date as event_date,
        cast(null as double) as expected_amount,
        s.gross_amount as settled_amount,
        cast(null as double) as diff_amount,
        case
            when s.gateway_ref_norm in (select natural_key from gateway_keys) then 'SOURCE_QUARANTINED'
            when s.gateway_ref_norm in (select txn_id from {{ source('silver', 'gateway_txns') }}) then 'SETTLED_NOT_CAPTURED'
            else 'ORPHAN_SETTLEMENT'
        end as status,
        cast(null as varchar) as match_method,
        false as is_duplicate_charge,
        cast(0 as double) as duplicate_exposure,
        cast(null as varchar) as order_issue,
        false as is_suspected_duplicate,
        cast(null as varchar) as duplicate_of,
        cast(null as varchar) as duplicate_basis,
        cast(0 as double) as suspected_duplicate_exposure
    from {{ source('silver', 'settlements') }} s
    where s.settlement_id not in (select settlement_id from {{ ref('int_txn_settlement_links') }})
),
order_items as (
    select
        'ORD:' || o.order_id as item_id,
        'order' as item_type,
        cast(null as varchar) as txn_id,
        cast(null as varchar) as settlement_id,
        o.order_id,
        o.merchant_id,
        cast(o.created_at as date) as event_date,
        o.order_amount as expected_amount,
        cast(null as double) as settled_amount,
        cast(null as double) as diff_amount,
        case when o.order_id in (select order_id from {{ source('quarantine', 'rows') }}
                                 where dataset = 'gateway' and order_id is not null)
             then 'PAYMENT_QUARANTINED' else 'ORDER_WITHOUT_PAYMENT' end as status,
        cast(null as varchar) as match_method,
        false as is_duplicate_charge,
        cast(0 as double) as duplicate_exposure,
        cast(null as varchar) as order_issue,
        false as is_suspected_duplicate,
        cast(null as varchar) as duplicate_of,
        cast(null as varchar) as duplicate_basis,
        cast(0 as double) as suspected_duplicate_exposure
    from {{ source('silver', 'orders') }} o
    where o.order_status in ('paid', 'refunded')
      and cast(o.created_at as date) + 2 < {{ as_of }}
      and o.order_id not in (select order_id from {{ ref('int_txns_captured') }}
                             where txn_type = 'payment' and order_id is not null)
),
unioned as (
    select * from txn_items
    union all select * from settlement_items
    union all select * from order_items
)
select
    *,
    case status
        when 'AMOUNT_MISMATCH' then abs(diff_amount)
        when 'MISSING_SETTLEMENT' then abs(expected_amount)
        when 'ORPHAN_SETTLEMENT' then abs(settled_amount)
        when 'SETTLED_NOT_CAPTURED' then abs(settled_amount)
        when 'ORPHAN_REFUND' then abs(expected_amount)
        when 'ORDER_WITHOUT_PAYMENT' then expected_amount
        else 0
    end as money_at_risk,
    {{ as_of }} as as_of_date
from unioned
