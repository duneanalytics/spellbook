{% set chain = 'tron' %}

{{
  config(
    tags = ['prod_exclude', 'static'],
    schema = 'tokens_' ~ chain,
    alias = 'trc20_stablecoins',
    materialized = 'view',
    unique_key = ['contract_address']
  )
}}

-- union view combining core (frozen) and extended (new additions) stablecoin lists

select blockchain, contract_address
from {{ ref('tokens_' ~ chain ~ '_trc20_stablecoins_core') }}

union all

select blockchain, contract_address
from {{ ref('tokens_' ~ chain ~ '_trc20_stablecoins_extended') }}
