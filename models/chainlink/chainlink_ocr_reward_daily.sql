{{
  config(
    tags=['dunesql'],
    alias='ocr_reward_daily',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                            "project",
                            "chainlink",
                            \'["linkpool_ryan"]\') }}'
  )
}}

{% set models = [
 'chainlink_ethereum_ocr_reward_daily'
] %}

SELECT *
FROM (
    {% for model in models %}
    SELECT
      blockchain,
      date_start,
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
