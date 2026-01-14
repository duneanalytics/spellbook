{% set chain = 'optimism' %}

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
-- add new stablecoins here (not in tokens_optimism_erc20_stablecoins_core)

select '{{chain}}' as blockchain, contract_address
from (values

     (0x0000000000000000000000000000000000000000)

     /* rebasing / interest accruing tokens
     (0x625e7708f30ca75bfd92586e17077590c60eb4cd), -- aOptUSDC (aave)
     (0x9dabae7274d28a45f0b65bf8ed201a5731492ca0), -- msUSD (morpho)
     (0xcb8fa9a76b8e203d8c3797bf438d8fb81ea3326a)  -- alUSD (alchemix)
     */

) as temp_table (contract_address)
where contract_address != 0x0000000000000000000000000000000000000000
