{{
  config(
    tags=['dunesql'],
    alias=alias('automation_performed_daily'),
    post_hook='{{ expose_spells(\'["ethereum", "avalanche_c", "bnb", "fantom", "polygon"]\',
                            "project",
                            "chainlink",
                            \'["linkpool_jon"]\') }}'
  )
}}

{% set models = [
  'chainlink_ethereum_automation_performed_daily',
  'chainlink_avalanche_c_automation_performed_daily',
  'chainlink_bnb_automation_performed_daily',
  'chainlink_fantom_automation_performed_daily',
  'chainlink_polygon_automation_performed_daily'
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
      token_amount       
    FROM {{ ref(model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
