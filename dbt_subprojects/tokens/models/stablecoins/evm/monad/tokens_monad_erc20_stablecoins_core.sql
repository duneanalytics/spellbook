{% set chain = 'monad' %}

{{
  config(
    schema = 'tokens_' ~ chain,
    alias = 'erc20_stablecoins_core',
    materialized = 'table',
    tags = ['static'],
    unique_key = ['contract_address']
  )
}}

-- core list: frozen stablecoin addresses used for initial incremental transfers
-- new stablecoins should be added to tokens_monad_erc20_stablecoins_extended

select '{{chain}}' as blockchain, contract_address, currency
from (values

     (0x754704bc059f8c67012fed69bc8a327a5aafb603, 'USD'), -- USDC
     (0x00000000efe302beaa2b3e6e1b18d08d69a9012a, 'USD'), -- AUSD
     (0xe7cd86e13ac4309349f30b3435a9d337750fc82d, 'USD'), -- USDT0
     (0x103222f020e98bba0ad9809a011fdf8e6f067496, 'USD'), -- earnAUSD
     (0x111111d2bf19e43c34263401e0cad979ed1cdb61, 'USD'), -- USD1
     (0xfd44b35139ae53fff7d8f2a9869c503d987f00d1, 'USD')  -- LVUSD

) as temp_table (contract_address, currency)
