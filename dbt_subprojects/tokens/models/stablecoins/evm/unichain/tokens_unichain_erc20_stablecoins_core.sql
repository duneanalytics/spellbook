{% set chain = 'unichain' %}

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
-- new stablecoins should be added to tokens_unichain_erc20_stablecoins_extended

select '{{chain}}' as blockchain, contract_address, currency
from (values

     (0x078d782b760474a361dda0af3839290b0ef57ad6, 'USD'), -- USDC
     (0x9151434b16b9763660705744891fa906f660ecc5, 'USD'), -- USD₮0
     (0x588ce4f028d8e7b53b687865d6a67b3a54c75518, 'USD'), -- USDT
     (0x1217bfe6c773eec6cc4a38b5dc45b92292b6e189, 'USD') -- oUSDT

) as temp_table (contract_address, currency)
