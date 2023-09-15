{{
  config(
    tags=['dunesql'],
    alias=alias('fm_reward_evt_transfer'),
    post_hook='{{ expose_spells(\'["arbitrum","avalanche_c","bnb","ethereum","fantom","gnosis","optimism","polygon"]\',
                            "project",
                            "chainlink",
                            \'["linkpool_jon"]\') }}'
  )
}}

{% set models = [
  'chainlink_arbitrum_fm_reward_evt_transfer',
  'chainlink_avalanche_c_fm_reward_evt_transfer',
  'chainlink_bnb_fm_reward_evt_transfer',
  'chainlink_ethereum_fm_reward_evt_transfer',
  'chainlink_fantom_fm_reward_evt_transfer',
  'chainlink_gnosis_fm_reward_evt_transfer',
  'chainlink_optimism_fm_reward_evt_transfer',
  'chainlink_polygon_fm_reward_evt_transfer'
] %}

SELECT *
FROM (
    {% for model in models %}
    SELECT
      blockchain,
      evt_block_time,
      admin_address,
      operator_name,
      token_value       
    FROM {{ ref(model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
