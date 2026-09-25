select
    cast(d as date) as date_key,
    extract(year from d) as year,
    extract(month from d) as month,
    strftime(d, '%Y-%m') as year_month,
    dayname(d) as day_name,
    extract(isodow from d) in (6, 7) as is_weekend
from generate_series(
    (select least(min(cast(created_at as date)), date '2026-01-01') from {{ source('silver', 'gateway_txns') }}),
    cast('{{ var("as_of_date") }}' as date) + 3,
    interval 1 day) as t(d)
