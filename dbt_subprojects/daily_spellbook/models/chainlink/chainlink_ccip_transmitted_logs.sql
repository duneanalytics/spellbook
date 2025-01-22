{{
  config(
    
    alias='ccip_transmitted_logs',
    post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "base", "bnb", "ethereum", "optimism", "polygon"]\',
                            "project",
                            "chainlink",
                            \'["linkpool_jon"]\') }}'
  )
}}

{% set models = [
  'chainlink_arbitrum_ccip_transmitted_logs',
  'chainlink_avalanche_c_ccip_transmitted_logs',
  'chainlink_base_ccip_transmitted_logs',
  'chainlink_bnb_ccip_transmitted_logs',
  'chainlink_ethereum_ccip_transmitted_logs',
  'chainlink_optimism_ccip_transmitted_logs',
  'chainlink_polygon_ccip_transmitted_logs'
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