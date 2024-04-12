{{
  config(
    
    alias='ccip_send_requested',
    post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "base", "bnb", "ethereum", "optimism", "polygon"]\',
                            "project",
                            "chainlink",
                            \'["linkpool_jon"]\') }}'
  )
}}

{% set models = [
  'chainlink_arbitrum_ccip_send_requested',
  'chainlink_avalanche_c_ccip_send_requested',
  'chainlink_base_ccip_send_requested',
  'chainlink_bnb_ccip_send_requested',
  'chainlink_ethereum_ccip_send_requested',
  'chainlink_optimism_ccip_send_requested',
  'chainlink_polygon_ccip_send_requested'
] %}

SELECT *
FROM (
    {% for model in models %}
    SELECT
      blockchain,
      evt_block_time,
      fee_token_amount,
      token,
      fee_token,
      destination_selector,
      destination_blockchain       
    FROM {{ ref(model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
