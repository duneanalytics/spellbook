{% set chain = 'katana' %}

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
-- new stablecoins should be added to tokens_katana_erc20_stablecoins_extended

select '{{chain}}' as blockchain, contract_address
from (values

     (0x203a662b0bd271a6ed5a60edfbd04bfce608fd36), -- vbUSDC
     (0x2dca96907fde857dd3d816880a0df407eeb2d2f2), -- vbUSDT
     (0x00000000efe302beaa2b3e6e1b18d08d69a9012a)  -- AUSD

) as temp_table (contract_address)
