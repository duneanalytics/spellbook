{% set chain = 'solana' %}

{{
  config(
    schema = 'tokens_' ~ chain,
    alias = 'spl_stablecoins_extended',
    materialized = 'table',
    tags = ['static'],
    unique_key = ['token_mint_address']
  )
}}

-- extended list: new stablecoin addresses added after the core list was frozen
-- add new stablecoins here (not in tokens_solana_spl_stablecoins_core)

select '{{chain}}' as blockchain, token_mint_address
from (values

     ('11111111111111111111111111111111')

) as temp_table (token_mint_address)
where token_mint_address != '11111111111111111111111111111111'
