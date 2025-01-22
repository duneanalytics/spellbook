{{
  config(
    
    alias='ocr_reward_transmission_logs',
    post_hook='{{ expose_spells(\'["arbitrum","avalanche_c","bnb","ethereum","fantom","gnosis","optimism","polygon"]\',
                            "project",
                            "chainlink",
                            \'["linkpool_ryan"]\') }}'
  )
}}

{% set models = [
  'chainlink_arbitrum_ocr_reward_transmission_logs',
  'chainlink_avalanche_c_ocr_reward_transmission_logs',
  'chainlink_bnb_ocr_reward_transmission_logs',
  'chainlink_ethereum_ocr_reward_transmission_logs',
  'chainlink_fantom_ocr_reward_transmission_logs',
  'chainlink_gnosis_ocr_reward_transmission_logs',
  'chainlink_optimism_ocr_reward_transmission_logs',
  'chainlink_polygon_ocr_reward_transmission_logs'
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