select * from {{ ref('int_exact_match') }}
union all
select * from {{ ref('int_fuzzy_match') }}
