{{
  config(
    
    alias='ocr_fulfilled_transactions',
    post_hook='{{ expose_spells(\'["arbitrum","avalanche_c","bnb","ethereum","fantom","gnosis","optimism","polygon"]\',
                            "project",
                            "chainlink",
                            \'["linkpool_ryan"]\') }}'
  )
}}

{% set models = [
  'chainlink_arbitrum_ocr_fulfilled_transactions',
  'chainlink_avalanche_c_ocr_fulfilled_transactions',
  'chainlink_bnb_ocr_fulfilled_transactions',
  'chainlink_ethereum_ocr_fulfilled_transactions',
  'chainlink_fantom_ocr_fulfilled_transactions',
  'chainlink_gnosis_ocr_fulfilled_transactions',
  'chainlink_optimism_ocr_fulfilled_transactions',
  'chainlink_polygon_ocr_fulfilled_transactions'
] %}

SELECT *
FROM (
    {% for model in models %}
    SELECT
      blockchain,
      block_time,
      date_month,
      node_address,
      token_amount,
      usd_amount,
      tx_hash,
      tx_index
    FROM {{ ref(model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)