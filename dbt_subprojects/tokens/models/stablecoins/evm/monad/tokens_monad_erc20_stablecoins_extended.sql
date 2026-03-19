{% set chain = 'monad' %}

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
-- add new stablecoins here (not in tokens_monad_erc20_stablecoins_core)

select '{{chain}}' as blockchain, contract_address, currency
from (values

     (0x09d4214c03d01f49544c0448dbe3a27f768f2b34, 'USD')  -- rUSD

     /* yield-bearing / rebasing tokens
     (0x103222f020e98bba0ad9809a011fdf8e6f067496, 'USD'), -- earnAUSD
     */

) as temp_table (contract_address, currency)
