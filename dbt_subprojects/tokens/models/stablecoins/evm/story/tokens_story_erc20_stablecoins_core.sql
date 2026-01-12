{% set chain = 'story' %}

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
-- new stablecoins should be added to tokens_story_erc20_stablecoins_extended

select '{{chain}}' as blockchain, contract_address
from (values

     (0xf1815bd50389c46847f0bda824ec8da914045d14)  -- USDC.e

) as temp_table (contract_address)
