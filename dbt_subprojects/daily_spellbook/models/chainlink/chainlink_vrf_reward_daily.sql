{{
  config(
    
    alias='vrf_reward_daily'
    , post_hook='{{ hide_spells() }}'
  )
}}

{% set models = [
  'chainlink_arbitrum_vrf_reward_daily',
  'chainlink_avalanche_c_vrf_reward_daily',
  'chainlink_bnb_vrf_reward_daily',
  'chainlink_ethereum_vrf_reward_daily',
  'chainlink_fantom_vrf_reward_daily',
  'chainlink_polygon_vrf_reward_daily'
] %}

SELECT *
FROM (
    {% for model in models %}
    SELECT
      blockchain,
      date_start,
      date_month,
      operator_address,
      token_amount,
      usd_amount
    FROM {{ ref(model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
