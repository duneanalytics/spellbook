{{
  config(
    
    alias='ccip_request_daily',
    post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "base", "bnb", "ethereum", "optimism", "polygon"]\',
                            "project",
                            "chainlink",
                            \'["linkpool_jon"]\') }}'
  )
}}

{% set models = [
  'chainlink_arbitrum_ccip_request_daily',
  'chainlink_avalanche_c_ccip_request_daily',
  'chainlink_base_ccip_request_daily',
  'chainlink_bnb_ccip_request_daily',
  'chainlink_ethereum_ccip_request_daily',
  'chainlink_optimism_ccip_request_daily',
  'chainlink_polygon_ccip_request_daily'
] %}

SELECT *
FROM (
    {% for model in models %}
    SELECT
      blockchain,
      date_start,
      date_month,
      fulfilled_requests,
      reverted_requests,
      total_requests
    FROM {{ ref(model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)