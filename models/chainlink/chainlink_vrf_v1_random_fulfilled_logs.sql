{{
  config(
    tags=['dunesql'],
    alias=alias('vrf_v1_random_fulfilled_logs'),
    post_hook='{{ expose_spells(\'["arbitrum","avalanche_c","bnb","ethereum","fantom","polygon"]\',
                            "project",
                            "chainlink",
                            \'["linkpool_jon"]\') }}'
  )
}}

{% set models = [
  'chainlink_arbitrum_vrf_v1_random_fulfilled_logs',
  'chainlink_avalanche_c_vrf_v1_random_fulfilled_logs',
  'chainlink_bnb_vrf_v1_random_fulfilled_logs',
  'chainlink_ethereum_vrf_v1_random_fulfilled_logs',
  'chainlink_fantom_vrf_v1_random_fulfilled_logs',
  'chainlink_polygon_vrf_v1_random_fulfilled_logs'
] %}

SELECT *
FROM (
    {% for model in models %}
    SELECT
      blockchain,
      block_hash,
      contract_address,
      data,
      topic0,
      topic1,
      topic2,
      topic3,
      tx_hash,
      block_number,
      block_time,
      index,
      tx_index
    FROM {{ ref(model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)