{{
  config(
    schema = 'lending_sonic',
    alias = 'base_market_hourly_agg',
    materialized = 'view'
  )
}}

{%
  set models = [
    ref('aave_v3_sonic_base_market_hourly_agg')
  ]
%}

{% for model in models %}
select
  blockchain,
  project,
  version,
  block_month,
  block_hour,
  token_address,
  symbol,
  liquidity_index,
  variable_borrow_index,
  deposit_rate,
  stable_borrow_rate,
  variable_borrow_rate
from {{ model }}
{% if not loop.last %}
union all
{% endif %}
{% endfor %}
