{% set chain = 'ink' %}

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
-- new stablecoins should be added to tokens_ink_erc20_stablecoins_extended

select '{{chain}}' as blockchain, contract_address
from (values

     (0x0000000000000000000000000000000000000000)

) as temp_table (contract_address)
where contract_address != 0x0000000000000000000000000000000000000000
