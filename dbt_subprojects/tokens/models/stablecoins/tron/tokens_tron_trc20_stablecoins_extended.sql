{% set chain = 'tron' %}

{{
  config(
    schema = 'tokens_' ~ chain,
    alias = 'trc20_stablecoins_extended',
    materialized = 'table',
    tags = ['prod_exclude', 'static'],
    unique_key = ['contract_address']
  )
}}

-- extended list: new stablecoin addresses added after the core list was frozen
-- add new stablecoins here (not in tokens_tron_trc20_stablecoins_core)

select '{{chain}}' as blockchain, contract_address
from (values

     ('T1111111111111111111111111111111111111111')

) as temp_table (contract_address)
where contract_address != 'T1111111111111111111111111111111111111111'
