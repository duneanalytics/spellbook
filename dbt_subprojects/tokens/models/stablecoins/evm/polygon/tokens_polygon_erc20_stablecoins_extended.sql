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

select '{{chain}}' as blockchain, contract_address
from (values

     (0x0000000000000000000000000000000000000000)

     /* rebasing / interest accruing tokens
     (0x3a3e7650f8b9f667da98f236010fbf44ee4b2975), -- xUSD (synthetic)
     (0x66f31345cb9477b427a1036d43f923a557c432a4)  -- iUSDS (iron bank)
     */

) as temp_table (contract_address)
where contract_address != 0x0000000000000000000000000000000000000000
