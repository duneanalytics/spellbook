{% set chains = [
    'arbitrum',
    'celo',
    'unichain',
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