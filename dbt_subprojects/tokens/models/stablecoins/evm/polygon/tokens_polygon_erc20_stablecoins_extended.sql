{% set chain = 'polygon' %}

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
-- add new stablecoins here (not in tokens_polygon_erc20_stablecoins_core)

select '{{chain}}' as blockchain, contract_address, currency
from (values

     (0x4fb71290ac171e1d144f7221d882becac7196eb5, 'TRY'), -- TRYB
     (0xd687759f35bb747a29246a4b9495c8f52c49e00c, 'AUD'), -- AUDX
     (0xd4dd9e2f021bb459d5a5f6c24c12fe09c5d45553, 'CHF')  -- ZCHF

     /* rebasing / interest accruing tokens
     (0x3a3e7650f8b9f667da98f236010fbf44ee4b2975), -- xUSD (synthetic)
     (0x66f31345cb9477b427a1036d43f923a557c432a4)  -- iUSDS (iron bank)
     */

) as temp_table (contract_address, currency)
