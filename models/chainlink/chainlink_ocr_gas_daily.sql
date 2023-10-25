{{
  config(
    
    alias='ocr_gas_daily',
    post_hook='{{ expose_spells(\'["arbitrum","avalanche_c","bnb","ethereum","fantom","gnosis","optimism","polygon"]\',
                            "project",
                            "chainlink",
                            \'["linkpool_ryan"]\') }}'
  )
}}

{% set models = [
  'chainlink_arbitrum_ocr_gas_daily',
  'chainlink_avalanche_c_ocr_gas_daily',
  'chainlink_bnb_ocr_gas_daily',
  'chainlink_ethereum_ocr_gas_daily',
  'chainlink_fantom_ocr_gas_daily',
  'chainlink_gnosis_ocr_gas_daily',
  'chainlink_optimism_ocr_gas_daily',
  'chainlink_polygon_ocr_gas_daily'
] %}

SELECT *
FROM (
    {% for model in models %}
    SELECT
      blockchain,
      date_start,
      date_month,
      node_address,
      operator_name,
      fulfilled_token_amount,
      fulfilled_usd_amount,
      reverted_token_amount,
      reverted_usd_amount,
      total_token_amount,
      total_usd_amount        
    FROM {{ ref(model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)