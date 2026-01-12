{% set chain = 'hemi' %}

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
-- new stablecoins should be added to tokens_hemi_erc20_stablecoins_extended

select '{{chain}}' as blockchain, contract_address
from (values

     (0xad11a8beb98bbf61dbb1aa0f6d6f2ecd87b35afa), -- USDC.e
     (0xbb0d083fb1be0a9f6157ec484b6c79e0a4e31c2e)  -- USDT

) as temp_table (contract_address)
