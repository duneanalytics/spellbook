{{
  config(
    tags=['dunesql'],
    alias=alias('automation_performed'),
    post_hook='{{ expose_spells(\'["ethereum"]\',
                            "project",
                            "chainlink",
                            \'["linkpool_jon"]\') }}'
  )
}}

{% set models = [
  'chainlink_ethereum_automation_performed'
] %}

SELECT *
FROM (
    {% for model in models %}
    SELECT
      blockchain,
      evt_block_time,
      keeper_address,
      operator_name,
      token_value       
    FROM {{ ref(model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
