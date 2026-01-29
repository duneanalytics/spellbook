{% set chain = 'zksync' %}

{{
  config(
    schema = 'tokens_' ~ chain,
    alias = 'erc20_stablecoins_core',
    materialized = 'table',
    tags = ['static'],
    unique_key = ['contract_address']
  )
}}

-- core list: frozen stablecoin addresses used for initial incremental balances
-- new stablecoins should be added to tokens_zksync_erc20_stablecoins_extended

select '{{chain}}' as blockchain, contract_address
from (values

     (0x3355df6d4c9c3035724fd0e3914de96a5a83aaf4), -- USDC.e
     (0x493257fd37edb34451f62edf8d2a0c418852ba4c), -- USDT
     (0x1d17cbcf0d6d143135ae902365d2e5e2a16538d4)  -- USDC

) as temp_table (contract_address)
