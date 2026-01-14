{% set chains = [
    'arbitrum',
    'avalanche_c',
    'base',
    'bnb',
    'celo',
    'ethereum',
    'kaia',
    'linea',
    'optimism',
    'polygon',
    'scroll',
    'unichain',
    'worldchain',
    'zksync',
] %}

{{
  config(
    schema = 'stablecoins_evm',
    alias = 'balances',
    materialized = 'view',
    post_hook = '{{ expose_spells(blockchains = \'["' ~ chains | join('","') ~ '"]\',
                                  spell_type = "sector",
                                  spell_name = "stablecoins",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

select *
from {{ ref('stablecoins_balances') }}