{% set chain = 'sei' %}

{{
  config(
    schema = 'tokens_' ~ chain,
    alias = 'erc20_stablecoins_core',
    materialized = 'table',
    tags = ['static'],
    unique_key = ['contract_address']
  )
}}

-- core list: frozen stablecoin addresses used for initial incremental transfers
-- new stablecoins should be added to tokens_sei_erc20_stablecoins_extended

select '{{chain}}' as blockchain, contract_address
from (values

     (0xe15fc38f6d8c56af07bbcbe3baf5708a2bf42392), -- USDC
     (0x3894085ef7ff0f0aedf52e2a2704928d1ec074f1), -- USDC
     (0x9151434b16b9763660705744891fa906f660ecc5)  -- USD₮0

) as temp_table (contract_address)
