{% set chain = 'worldchain' %}

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
-- add new stablecoins here (not in tokens_worldchain_erc20_stablecoins_core)

select '{{chain}}' as blockchain, contract_address, currency
from (values

     (0x8a1d45e102e886510e891d2ec656a708991e2d76, 'COP'), -- wCOP
     (0x61d450a098b6a7f69fc4b98ce68198fe59768651, 'CLP'), -- wCLP
     (0x4f34c8b3b5fb6d98da888f0fea543d4d9c9f2ebe, 'PEN'), -- wPEN
     (0xd3fd63209fa2d55b07a0f6db36c2f43900be3094, 'USD')  -- wsrUSD

) as temp_table (contract_address, currency)
