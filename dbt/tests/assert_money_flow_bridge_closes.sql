-- The bank difference of a chain is exactly the sum of its three components, and the gateway gap is gateway minus expectation.
select flow_id
from {{ source('recon', 'fact_money_flow') }}
where abs(difference_amount - (gap_bank_amount + gap_fee + gap_bank_net)) > 0.02
   or abs(gap_gateway - (coalesce(gateway_gross, 0) - coalesce(expected_gateway, 0))) > 0.011
   or merchant_amount is not null
