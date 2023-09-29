{{
  config(
    tags=['dunesql'],
    alias=alias('automation_request_daily'),
    post_hook='{{ expose_spells(\'["ethereum", "avalanche_c", "bnb", "fantom", "polygon"]\',
                            "project",
                            "chainlink",
                            \'["linkpool_jon"]\') }}'
  )
}}

{% set models = [
  'chainlink_ethereum_automation_request_daily',
  'chainlink_avalanche_c_automation_request_daily',
  'chainlink_bnb_automation_request_daily',
  'chainlink_fantom_automation_request_daily',
  'chainlink_polygon_automation_request_daily'
] %}

SELECT *
FROM (
    {% for model in models %}
    SELECT
      blockchain,
      date_start,
      date_month,
      node_address,
      operator_name,
      fulfilled_requests,
      reverted_requests,
      total_requests
    FROM {{ ref(model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)