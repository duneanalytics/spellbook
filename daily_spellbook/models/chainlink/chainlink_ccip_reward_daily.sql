{{
  config(
    
    alias='ccip_reward_daily',
    post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "base", "bnb", "ethereum", "optimism", "polygon"]\',
                            "project",
                            "chainlink",
                            \'["linkpool_jon"]\') }}'
  )
}}

{% set models = [
  'chainlink_arbitrum_ccip_reward_daily',
  'chainlink_avalanche_c_ccip_reward_daily',
  'chainlink_base_ccip_reward_daily',
  'chainlink_bnb_ccip_reward_daily',
  'chainlink_ethereum_ccip_reward_daily',
  'chainlink_optimism_ccip_reward_daily',
  'chainlink_polygon_ccip_reward_daily'
] %}

SELECT *
FROM (
    {% for model in models %}
    SELECT
      blockchain,
      date_start,
      date_month,
      token_amount,
      usd_amount,
      token
    FROM {{ ref(model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
