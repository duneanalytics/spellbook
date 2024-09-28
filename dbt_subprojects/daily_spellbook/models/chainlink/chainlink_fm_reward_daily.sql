{{
  config(
    
    alias='fm_reward_daily',
    post_hook='{{ expose_spells(\'["arbitrum","avalanche_c","bnb","ethereum","fantom","gnosis","optimism","polygon"]\',
                            "project",
                            "chainlink",
                            \'["linkpool_jon"]\') }}'
  )
}}

{% set models = [
  'chainlink_arbitrum_fm_reward_daily',
  'chainlink_avalanche_c_fm_reward_daily',
  'chainlink_bnb_fm_reward_daily',
  'chainlink_ethereum_fm_reward_daily',
  'chainlink_fantom_fm_reward_daily',
  'chainlink_gnosis_fm_reward_daily',
  'chainlink_optimism_fm_reward_daily',
  'chainlink_polygon_fm_reward_daily'
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
      token_amount,
      usd_amount
    FROM {{ ref(model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
