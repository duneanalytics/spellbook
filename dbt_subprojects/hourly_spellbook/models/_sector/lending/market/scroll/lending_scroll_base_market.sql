{{
  config(
    schema = 'lending_scroll',
    alias = 'base_market',
    materialized = 'view'
  )
}}

{%
  set models = [
    ref('aave_v3_scroll_base_market')
  ]
%}

{% for model in models %}
select
  blockchain,
  project,
  version,
  block_time,
  block_hour,
  block_month,
  block_number,
  token_address,
  symbol,
  liquidity_index,
  variable_borrow_index,
  deposit_rate,
  stable_borrow_rate,
  variable_borrow_rate,
  project_contract_address,
  evt_index,
  tx_hash
from {{ model }}
{% if not loop.last %}
union all
{% endif %}
{% endfor %}
