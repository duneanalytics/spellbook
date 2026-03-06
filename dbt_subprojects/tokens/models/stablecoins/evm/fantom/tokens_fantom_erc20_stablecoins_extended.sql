{% set chain = 'fantom' %}

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
-- add new stablecoins here (not in tokens_fantom_erc20_stablecoins_core)

select '{{chain}}' as blockchain, contract_address, currency
from (values

     (0x0000000000000000000000000000000000000000, 'USD')

     /* rebasing / interest accruing tokens
     (0xb67fa6defce4042070eb1ae1511dcd6dcc6a532e), -- alUSD (alchemix synthetic)
     (0x1d3918043d22de2d799a4d80f72efd50db90b5af)  -- sPDO (staked PDO)
     */

) as temp_table (contract_address, currency)
where contract_address != 0x0000000000000000000000000000000000000000
