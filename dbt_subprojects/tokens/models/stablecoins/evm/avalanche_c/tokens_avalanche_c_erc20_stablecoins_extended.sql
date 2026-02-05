{% set chain = 'avalanche_c' %}

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
-- add new stablecoins here (not in tokens_avalanche_c_erc20_stablecoins_core)

select '{{chain}}' as blockchain, contract_address
from (values

     (0x564a341df6c126f90cf3ecb92120fd7190acb401), -- TRYB
     (0xd4dd9e2f021bb459d5a5f6c24c12fe09c5d45553)  -- ZCHF

     /* rebasing / interest accruing tokens
     (0xabe7a9dfda35230ff60d1590a929ae0644c47dc1), -- aUSD (aave)
     (0x8861f5c40a0961579689fdf6cdea2be494f9b25a)  -- iUSDS (iron bank)
     */

) as temp_table (contract_address)
