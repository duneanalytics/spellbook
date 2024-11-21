{{
  config(
    
    alias='ocr_reward_evt_transfer_daily',
    post_hook='{{ expose_spells(\'["arbitrum","avalanche_c","bnb","ethereum","fantom","gnosis","optimism","polygon"]\',
                            "project",
                            "chainlink",
                            \'["linkpool_ryan"]\') }}'
  )
}}

{% set models = [
  'chainlink_arbitrum_ocr_reward_evt_transfer_daily',
  'chainlink_avalanche_c_ocr_reward_evt_transfer_daily',
  'chainlink_bnb_ocr_reward_evt_transfer_daily',
  'chainlink_ethereum_ocr_reward_evt_transfer_daily',
  'chainlink_fantom_ocr_reward_evt_transfer_daily',
  'chainlink_gnosis_ocr_reward_evt_transfer_daily',
  'chainlink_optimism_ocr_reward_evt_transfer_daily',
  'chainlink_polygon_ocr_reward_evt_transfer_daily'
] %}

SELECT *
FROM (
    {% for model in models %}
    SELECT
      blockchain,
      date_start,
      date_month,
      admin_address,
      operator_name,
      token_amount       
    FROM {{ ref(model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
