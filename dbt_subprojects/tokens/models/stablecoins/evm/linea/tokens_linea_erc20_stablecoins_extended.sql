{% set chain = 'linea' %}

{{
  config(
    schema = 'tokens_' ~ chain,
    alias = 'erc20_stablecoins_extended',
    materialized = 'table',
    tags = ['static'],
    unique_key = ['contract_address']
  )
}}

-- extended list: new stablecoin addresses added after the core list was frozen
-- add new stablecoins here (not in tokens_linea_erc20_stablecoins_core)

select '{{chain}}' as blockchain, contract_address
from (values

     (0x0000000000000000000000000000000000000000)

) as temp_table (contract_address)
where contract_address != 0x0000000000000000000000000000000000000000
