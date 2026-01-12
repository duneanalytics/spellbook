{% set chain = 'plasma' %}

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
-- new stablecoins should be added to tokens_plasma_erc20_stablecoins_extended

select '{{chain}}' as blockchain, contract_address
from (values

     (0xb8ce59fc3717ada4c02eadf9682a9e934f625ebb), -- USDT0
     (0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34), -- USDe
     (0x0a1a1a107e45b7ced86833863f482bc5f4ed82ef)  -- USDai

) as temp_table (contract_address)
