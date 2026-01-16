{% set chain = 'base' %}

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
-- add new stablecoins here (not in tokens_base_erc20_stablecoins_core)

select '{{chain}}' as blockchain, contract_address
from (values

     (0x0000000000000000000000000000000000000000)

     /* rebasing / interest accruing tokens
     (0x526728dbc96689597f85ae4cd716d4f7fccbae9d), -- msUSD (morpho)
     (0x4e65fe4dba92790696d040ac24aa414708f5c0ab)  -- aBasUSDC (aave)
     */

) as temp_table (contract_address)
where contract_address != 0x0000000000000000000000000000000000000000
