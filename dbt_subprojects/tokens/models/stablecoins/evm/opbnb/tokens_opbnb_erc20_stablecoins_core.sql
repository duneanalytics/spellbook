{% set chain = 'opbnb' %}

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
-- new stablecoins should be added to tokens_opbnb_erc20_stablecoins_extended

select '{{chain}}' as blockchain, contract_address, currency
from (values

     (0x9e5aac1ba1a2e6aed6b32689dfcf62a509ca96f3, 'USD')  -- USDT

) as temp_table (contract_address, currency)
