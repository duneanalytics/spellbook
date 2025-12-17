{% set chain = 'blast' %}

{{
  config(
    schema = 'tokens_' ~ chain,
    alias = 'erc20_stablecoins',
    tags = ['static'],
    unique_key = ['contract_address']
  )
}}

-- union view combining core (frozen) and extended (new additions) stablecoin lists

select blockchain, contract_address, backing, symbol, decimals, name
from {{ ref('tokens_' ~ chain ~ '_erc20_stablecoins_core') }}

union all

select blockchain, contract_address, backing, symbol, decimals, name
from {{ ref('tokens_' ~ chain ~ '_erc20_stablecoins_extended') }}
