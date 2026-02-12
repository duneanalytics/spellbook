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

     (0x0000000000000000000000000000000000000000, 'USD')

     /* yield-bearing / rebasing tokens
     (0x103222f020e98bba0ad9809a011fdf8e6f067496, 'USD'), -- earnAUSD
     */

) as temp_table (contract_address, currency)
where contract_address != 0x0000000000000000000000000000000000000000
