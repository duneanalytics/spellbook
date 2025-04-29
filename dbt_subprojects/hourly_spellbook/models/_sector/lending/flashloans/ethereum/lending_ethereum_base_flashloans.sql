{{
  config(
    schema = 'lending_ethereum',
    alias = 'base_flashloans',
    materialized = 'view'
  )
}}

{%
  set models = [
    ref('aave_v1_ethereum_base_flashloans'),
    ref('aave_v2_ethereum_base_flashloans'),
    ref('aave_v3_ethereum_base_flashloans'),
    ref('aave_lido_v3_ethereum_base_flashloans'),
    ref('aave_etherfi_v3_ethereum_base_flashloans'),
    ref('radiant_ethereum_base_flashloans'),
    ref('uwulend_ethereum_base_flashloans'),
    ref('spark_ethereum_base_flashloans'),
    ref('granary_ethereum_base_flashloans'),
    ref('balancer_v2_ethereum_base_flashloans'),
    ref('morpho_ethereum_base_flashloans')
  ]
%}

{% for model in models %}
select
  blockchain,
  project,
  version,
  recipient,
  amount,
  fee,
  token_address,
  project_contract_address,
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
