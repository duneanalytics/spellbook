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

     (0x866d66f64fb81461903e1e38d998e747ecf35e78, 'USD'), -- rUSD
     (0x061af032ccf1ce35a39b556e0f442bf2dbe1ed06, 'USD')  -- CASH

) as temp_table (contract_address, currency)
