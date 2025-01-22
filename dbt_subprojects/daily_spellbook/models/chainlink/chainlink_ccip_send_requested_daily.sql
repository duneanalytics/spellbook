{{
  config(
    
    alias='ccip_send_requested_daily',
    post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "base", "bnb", "ethereum", "optimism", "polygon"]\',
                            "project",
                            "chainlink",
                            \'["linkpool_jon"]\') }}'
  )
}}

{% set models = [
  'chainlink_arbitrum_ccip_send_requested_daily',
  'chainlink_avalanche_c_ccip_send_requested_daily',
  'chainlink_base_ccip_send_requested_daily',
  'chainlink_bnb_ccip_send_requested_daily',
  'chainlink_ethereum_ccip_send_requested_daily',
  'chainlink_optimism_ccip_send_requested_daily',
  'chainlink_polygon_ccip_send_requested_daily'
] %}

SELECT *
FROM (
    {% for model in models %}
    SELECT
      blockchain,
      date_start,
      date_month,
      fee_amount,
      token,
      destination_blockchain,
      count
    FROM {{ ref(model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)