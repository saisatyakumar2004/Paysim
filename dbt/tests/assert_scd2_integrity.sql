-- SCD2 invariants: exactly one current row per merchant, and no overlapping validity windows.
with current_counts as (
    select merchant_id from {{ ref('dim_merchant') }}
    group by merchant_id having count(*) filter (where is_current) <> 1
),
overlapping as (
    select a.merchant_id
    from {{ ref('dim_merchant') }} a
    join {{ ref('dim_merchant') }} b
      on a.merchant_id = b.merchant_id and a.valid_from < b.valid_from and a.valid_to > b.valid_from
)
select merchant_id from current_counts
union all
select merchant_id from overlapping
