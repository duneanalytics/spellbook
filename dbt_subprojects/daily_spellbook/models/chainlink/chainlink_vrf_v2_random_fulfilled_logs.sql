{{
  config(
    
    alias='vrf_v2_random_fulfilled_logs'
    , post_hook='{{ hide_spells() }}'
  )
}}

{% set models = [
  'chainlink_arbitrum_vrf_v2_random_fulfilled_logs',
  'chainlink_avalanche_c_vrf_v2_random_fulfilled_logs',
  'chainlink_bnb_vrf_v2_random_fulfilled_logs',
  'chainlink_ethereum_vrf_v2_random_fulfilled_logs',
  'chainlink_fantom_vrf_v2_random_fulfilled_logs',
  'chainlink_polygon_vrf_v2_random_fulfilled_logs'
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