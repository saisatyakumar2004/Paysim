-- Every bank record belongs to exactly one money chain, so the chains must add up to the bank file to the cent.
select a.chain_total, b.silver_total
from (select coalesce(sum(bank_net), 0) as chain_total from {{ source('recon', 'fact_money_flow') }}) a,
     (select coalesce(sum(net_amount), 0) as silver_total from {{ source('silver', 'settlements') }}) b
where abs(a.chain_total - b.silver_total) > 0.01
