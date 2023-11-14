{{
  config(
    schema = 'mento_celo',
    alias = 'pools',
    materialized = 'view'
  )
}}

{% set pool_models = [
  ref('mento_celo_pools_v1'),
  ref('mento_celo_pools_v2')
] %}

{% for pool_model in pool_models %}
select
  blockchain,
  project,
  version,
  pool,
  fee,
  token0,
  token1,
  creation_block_time,
  creation_block_number,
  contract_address
from {{ pool_model }}
{% if not loop.last %}
union all
{% endif %}
{% endfor %}
