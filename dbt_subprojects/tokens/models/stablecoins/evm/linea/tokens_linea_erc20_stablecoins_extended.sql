{% set chain = 'linea' %}

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
-- add new stablecoins here (not in tokens_linea_erc20_stablecoins_core)

select '{{chain}}' as blockchain, contract_address, currency
from (values

     (0x0000000000000000000000000000000000000000, 'USD')

     /* yield-bearing / rebasing tokens
     (0xb79dd08ea68a908a97220c76d19a6aa9cbde4376, 'USD'), -- USD+
     (0x1e1f509963a6d33e169d9497b11c7dbfe73b7f13, 'USD'), -- USDT+
     */

     /* rebasing / interest accruing tokens
     (0xaca92e438df0b2401ff60da7e4337b687a2435da)  -- mUSD (morpho)
     */

) as temp_table (contract_address, currency)
where contract_address != 0x0000000000000000000000000000000000000000
