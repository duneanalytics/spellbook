{% set chain = 'abstract' %}

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
-- new stablecoins should be added to tokens_abstract_erc20_stablecoins_extended

select '{{chain}}' as blockchain, contract_address
from (values

     (0x84a71ccd554cc1b02749b35d22f684cc8ec987e1), -- USDC.e
     (0x0709f39376deee2a2dfc94a58edeb2eb9df012bd)  -- USDT

) as temp_table (contract_address)
