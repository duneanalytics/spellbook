{{
  config(
    schema = 'lending_optimism',
    alias = 'base_supply',
    materialized = 'view'
  )
}}

{%
  set models = [
    ref('aave_v3_optimism_base_supply'),
    ref('sonne_finance_optimism_base_supply'),
    ref('granary_optimism_base_supply')
  ]
%}

{% for model in models %}
select
  blockchain,
  project,
  version,
  transaction_type,
  token_address,
  depositor,
  withdrawn_to,
  liquidator,
  amount,
  block_month,
  block_time,
  block_number,
  tx_hash,
  evt_index
from {{ model }}
{% if not loop.last %}
union all
{% endif %}
{% endfor %}
