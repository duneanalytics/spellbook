{{
  config(
    tags = ['dunesql'],
    schema = 'mento_celo',
    alias = alias('pools'),
    post_hook = '{{ expose_spells(\'["celo"]\',
                                "project",
                                "mento",
                                \'["tomfutago"]\') }}'
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
  contract_address,
  token_pair,
  asset0,
  asset1,
  exchange_id,
  is_active,
  block_time_created,
  block_number_created,
  block_time_destroyed,
  block_number_destroyed
from {{ pool_model }}
{% if not loop.last %}
union all
{% endif %}
{% endfor %}
