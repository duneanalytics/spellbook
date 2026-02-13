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

select '{{chain}}' as blockchain, contract_address, currency
from (values

     (0x4933a85b5b5466fbaf179f72d3de273c287ec2c2, 'EUR'), -- EURAU
     (0xd4dd9e2f021bb459d5a5f6c24c12fe09c5d45553, 'CHF')  -- ZCHF

     /* yield-bearing / rebasing tokens
     (0x970d50d09f3a656b43e11b0d45241a84e3a6e011, 'USD'), -- DAI+
     (0x340fe1d898eccaad394e2ba0fc1f93d27c7b717a, 'USD'), -- wUSDR
     (0x73cb180bf0521828d8849bc8cf2b920918e23032, 'USD'), -- USD+
     (0x59d9356e565ab3a36dd77763fc0d87feaf85508c, 'USD'), -- USDM
     */

     /* rebasing / interest accruing tokens
     (0x625e7708f30ca75bfd92586e17077590c60eb4cd), -- aOptUSDC (aave)
     (0x9dabae7274d28a45f0b65bf8ed201a5731492ca0), -- msUSD (morpho)
     (0xcb8fa9a76b8e203d8c3797bf438d8fb81ea3326a)  -- alUSD (alchemix)
     */

) as temp_table (contract_address, currency)
