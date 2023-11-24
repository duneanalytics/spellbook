{{
  config(
    schema = 'lending',
    alias = 'base_flashloans',
    partition_by = ['blockchain', 'project', 'block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
  )
}}

{%
  set models = [
    ref('lending_arbitrum_base_flashloans'),
    ref('lending_base_base_flashloans'),
    ref('lending_celo_base_flashloans'),
    ref('lending_ethereum_base_flashloans'),
    ref('lending_optimism_base_flashloans'),
    ref('lending_polygon_base_flashloans'),
    ref('lending_avalanche_c_base_flashloans'),
    ref('lending_fantom_base_flashloans')
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
  contract_address,
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
