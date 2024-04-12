{{
  config(
    schema = 'lending_ethereum',
    alias = 'base_supply',
    materialized = 'view'
  )
}}

{%
  set models = [
    ref('aave_v1_ethereum_base_supply'),
    ref('aave_v2_ethereum_base_supply'),
    ref('aave_v3_ethereum_base_supply'),
    ref('compound_v2_ethereum_base_supply'),
    ref('compound_v3_ethereum_base_supply'),
    ref('radiant_ethereum_base_supply'),
    ref('uwulend_ethereum_base_supply'),
    ref('spark_ethereum_base_supply'),
    ref('fluxfinance_ethereum_base_supply'),
    ref('strike_ethereum_base_supply'),
    ref('granary_ethereum_base_supply')
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
