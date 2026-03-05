{% set chain = 'hyperevm' %}

{{
  config(
    schema = 'tokens_' ~ chain,
    alias = 'erc20_stablecoins_extended',
    materialized = 'table',
    tags = ['static'],
    unique_key = ['contract_address']
  )
}}

-- extended list: new stablecoin addresses added after the core list was frozen
-- add new stablecoins here (not in tokens_hyperevm_erc20_stablecoins_core)

select '{{chain}}' as blockchain, contract_address, currency
from (values

     (0xd3fd63209fa2d55b07a0f6db36c2f43900be3094, 'USD')  -- wsrUSD

) as temp_table (contract_address, currency)
