{{
  config(
    schema = 'lending_arbitrum',
    alias = 'base_liquidity_pool',
    materialized = 'view'
  )
}}

{%
  set models = [
    ref('aave_v3_arbitrum_base_liquidity_pool')
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
