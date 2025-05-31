{{
  config(
    schema = 'lending_fantom',
    alias = 'base_supply_scaled',
    materialized = 'view'
  )
}}

{%
  set models = [
    ref('aave_v3_fantom_base_supply_scaled')
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
  user,
  amount
from {{ model }}
{% if not loop.last %}
union all
{% endif %}
{% endfor %} 