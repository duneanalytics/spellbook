{% set chain = 'ink' %}

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
-- new stablecoins should be added to tokens_ink_erc20_stablecoins_extended

select '{{chain}}' as blockchain, contract_address, currency
from (values

     (0x0200c29006150606b650577bbe7b6248f58470c1, 'USD'), -- USD₮0
     (0xe343167631d89b6ffc58b88d6b7fb0228795491d, 'USD'), -- USDG
     (0x2d270e6886d130d724215a266106e6832161eaed, 'USD'), -- USDC
     (0xf1815bd50389c46847f0bda824ec8da914045d14, 'USD'), -- USDC.e
     (0x1217bfe6c773eec6cc4a38b5dc45b92292b6e189, 'USD')  -- oUSDT

) as temp_table (contract_address, currency)
