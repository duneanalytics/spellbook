{% set chain = 'sonic' %}

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
-- new stablecoins should be added to tokens_sonic_erc20_stablecoins_extended

select '{{chain}}' as blockchain, contract_address
from (values

     (0x29219dd400f2bf60e5a23d13be72b486d4038894), -- USDC.e
     (0xd3dce716f3ef535c5ff8d041c1a41c3bd89b97ae), -- scUSD
     (0x6047828dc181963ba44974801ff68e538da5eaf9), -- USDT
     (0x6646248971427b80ce531bdd793e2eb859347e55)  -- waSonUSDC

) as temp_table (contract_address)
