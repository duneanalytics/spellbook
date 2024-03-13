{{
  config(
    schema = 'lending_arbitrum',
    alias = 'base_supply',
    materialized = 'view'
  )
}}

{%
  set models = [
    ref('aave_v3_arbitrum_base_supply'),
    ref('compound_v3_arbitrum_base_supply'),
    ref('radiant_arbitrum_base_supply'),
    ref('lodestar_v0_arbitrum_base_supply'),
    ref('lodestar_v1_arbitrum_base_supply'),
    ref('granary_arbitrum_base_supply')
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
