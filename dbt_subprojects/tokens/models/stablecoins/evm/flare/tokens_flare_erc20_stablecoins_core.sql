{% set chain = 'flare' %}

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
-- new stablecoins should be added to tokens_flare_erc20_stablecoins_extended

select '{{chain}}' as blockchain, contract_address
from (values

     (0xe7cd86e13ac4309349f30b3435a9d337750fc82d), -- USD₮0
     (0xfbda5f676cb37624f28265a144a48b0d6e87d3b6), -- USDC.e
     (0x0b38e83b86d491735feaa0a791f65c2b99535396)  -- USDT

) as temp_table (contract_address)
