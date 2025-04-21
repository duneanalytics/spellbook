{{
  config(
    schema = 'lending',
    alias = 'liquidity_pool',
    materialized = 'view',
    unique_key = ['blockchain', 'project', 'version', 'block_date', 'wallet_address', 'token_address'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    post_hook = '{{ expose_spells(blockchains = \'["arbitrum", "avalanche_c", "base", "bnb", "celo", "ethereum", "fantom", "gnosis", "linea", "optimism", "polygon", "scroll", "sonic", "zksync"]\',
                                  spell_type = "sector",
                                  spell_name = "lending",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

{%
  set models = [
    ref('lending_arbitrum_base_liquidity_pool')
  ]
%}

{% for model in models %}
select
  blockchain,
  project,
  version,
  block_date,
  wallet_address,
  token_address,
  token_symbol,
  supplied_amount,
  borrowed_amount
from {{ model }}
{% if not loop.last %}
union all
{% endif %}
{% endfor %}
