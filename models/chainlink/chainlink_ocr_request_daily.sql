{{
  config(
    tags=['dunesql'],
    alias=alias('ocr_request_daily'),
    post_hook='{{ expose_spells(\'["arbitrum","avalanche_c","bnb","ethereum","fantom","gnosis","optimism","polygon"]\',
                            "project",
                            "chainlink",
                            \'["linkpool_ryan"]\') }}'
  )
}}

{% set models = [
  'chainlink_arbitrum_ocr_request_daily',
  'chainlink_avalanche_c_ocr_request_daily',
  'chainlink_bnb_ocr_request_daily',
  'chainlink_ethereum_ocr_request_daily',
  'chainlink_fantom_ocr_request_daily',
  'chainlink_gnosis_ocr_request_daily',
  'chainlink_optimism_ocr_request_daily',
  'chainlink_polygon_ocr_request_daily'
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
      fulfilled_requests,
      reverted_requests,
      total_requests
    FROM {{ ref(model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)