{% set chain = 'xlayer' %}

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
-- add new stablecoins here (not in tokens_xlayer_erc20_stablecoins_core)

select '{{chain}}' as blockchain, contract_address, currency
from (values

     (0x74b7f16337b8972027f6196a17a631ac6de26d22, 'USD') -- USDC

) as temp_table (contract_address, currency)
