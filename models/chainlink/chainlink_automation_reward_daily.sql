{{
  config(
    
    alias='automation_reward_daily',
    post_hook='{{ expose_spells(\'["ethereum", "avalanche_c", "bnb", "fantom", "polygon"]\',
                            "project",
                            "chainlink",
                            \'["linkpool_jon"]\') }}'
  )
}}

{% set models = [
  'chainlink_ethereum_automation_reward_daily',
  'chainlink_avalanche_c_automation_reward_daily',
  'chainlink_bnb_automation_reward_daily',
  'chainlink_fantom_automation_reward_daily',
  'chainlink_polygon_automation_reward_daily'
] %}

SELECT *
FROM (
    {% for model in models %}
    SELECT
      blockchain,
      date_start,
      date_month,
      keeper_address,
      operator_name,
      token_amount,
      usd_amount
    FROM {{ ref(model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
