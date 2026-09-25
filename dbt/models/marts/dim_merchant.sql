-- SCD Type 2 merchant dimension. One row per merchant VERSION; facts join on the version valid at event time.
select
    md5(merchant_id || '|' || cast(valid_from as varchar)) as merchant_sk,
    merchant_id, name, category, city, risk_tier, fee_pct,
    valid_from, valid_to, is_current
from {{ source('silver', 'merchants_scd2') }}
