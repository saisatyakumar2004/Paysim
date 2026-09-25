{{ config(tags=['reference']) }}
-- REFERENCE IMPLEMENTATION (full rebuild). The daily pipeline uses the incremental engine in
-- src/payreconcile/reconcile.py; this model is the independent oracle used by `verify` and the benchmark.
-- One row per reconciliation item across the three-way match (gateway <-> bank <-> orders).
--   item_type = txn         a captured gateway movement (payment or refund)
--   item_type = settlement  bank money that could not be linked to any gateway txn
--   item_type = order       a paid order with no captured payment
{% set as_of = "cast('" ~ var('as_of_date') ~ "' as date)" %}

with gateway_keys as (
    select natural_key from {{ source('quarantine', 'rows') }}
    where dataset = 'gateway' and natural_key is not null
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
        s.gross_amount as settled_amount,
        s.gross_amount - t.expected_gross as diff_amount,
        case
            when t.txn_type = 'refund'
                 and t.refund_of not in (select txn_id from {{ source('silver', 'gateway_txns') }})
                 and t.refund_of not in (select natural_key from gateway_keys)
                then 'ORPHAN_REFUND'
            when l.settlement_id is null then
                case when t.txn_date + {{ var('sla_days') }} < {{ as_of }} then 'MISSING_SETTLEMENT' else 'PENDING' end
            when abs(s.gross_amount - t.expected_gross) <= {{ var('amount_tolerance') }} then 'MATCHED'
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
        end as order_issue
    from {{ ref('int_txns_captured') }} t
    left join {{ ref('int_txn_settlement_links') }} l on l.txn_id = t.txn_id
    left join {{ source('silver', 'settlements') }} s on s.settlement_id = l.settlement_id
    left join {{ ref('int_duplicate_charges') }} d on d.txn_id = t.txn_id
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
        cast(null as varchar) as order_issue
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
        cast(null as varchar) as order_issue
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
