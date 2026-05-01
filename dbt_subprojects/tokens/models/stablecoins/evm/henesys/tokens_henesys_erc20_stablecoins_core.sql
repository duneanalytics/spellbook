{% set chain = 'henesys' %}

{{
  config(
    schema = 'tokens_' ~ chain,
    alias = 'erc20_stablecoins_core',
    materialized = 'table',
    tags = ['prod_exclude', 'static'],
    unique_key = ['contract_address']
  )
}}

-- core list: frozen stablecoin addresses used for initial incremental transfers
-- new stablecoins should be added to tokens_henesys_erc20_stablecoins_extended

select '{{chain}}' as blockchain, contract_address, currency
from (values

     (0x0000000000000000000000000000000000000000, 'USD')

) as temp_table (contract_address, currency)
where contract_address != 0x0000000000000000000000000000000000000000
