{{
  config(
    tags=['dunesql'],
    alias='ocr_reward_evt_transfer',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                            "project",
                            "chainlink",
                            \'["linkpool_ryan"]\') }}'
  )
}}

{% set models = [
 'chainlink_ethereum_ocr_reward_evt_transfer'
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
