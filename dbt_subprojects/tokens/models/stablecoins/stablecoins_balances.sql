{% set chains = [
    'arbitrum',
    'avalanche_c',
    'base',
    'bnb',
    'ethereum',
    'kaia',
    'linea',
    'optimism',
    'polygon',
    'scroll',
    'worldchain',
    'zksync',
] %}

{{
  config(
    schema = 'stablecoins',
    alias = 'balances',
    materialized = 'view',
    post_hook = '{{ expose_spells(blockchains = \'["' ~ chains | join('","') ~ '"]\',
                                  spell_type = "sector",
                                  spell_name = "stablecoins",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

select *
from (
    {% for chain in chains %}
    select
        blockchain,
        day,
        address,
        token_symbol,
        token_address,
        token_standard,
        token_id,
        balance_raw,
        balance,
        balance_usd,
        last_updated
    from {{ ref('stablecoins_' ~ chain ~ '_balances') }}
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
)
