{{
  config(
    schema = 'lending_arbitrum',
    alias = 'base_reserve',
    materialized = 'view'
  )
}}

{%
  set models = [
    ref('aave_v3_arbitrum_base_reserve')
  ]
%}

{% for model in models %}
select
  blockchain,
  project,
  version,
  block_time,
  block_date,
  block_month,
  block_number,
  token_address,
  token_symbol,
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
