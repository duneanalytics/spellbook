{% set chain = 'gnosis' %}

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
-- add new stablecoins here (not in tokens_gnosis_erc20_stablecoins_core)

select '{{chain}}' as blockchain, contract_address, currency
from (values

     (0x8e34bfec4f6eb781f9743d9b4af99cd23f9b7053, 'GBP'), -- GBPe (new contract)
     (0x5cb9073902f2035222b9749f8fb0c9bfe5527108, 'GBP'), -- GBPe (old contract)
     (0xcb444e90d8198415266c6a2724b7900fb12fc56e, 'EUR')  -- EURe

) as temp_table (contract_address, currency)
