{{
  config(
    tags=['dunesql'],
    alias='ocr_fulfilled_transactions',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                            "project",
                            "chainlink",
                            \'["linkpool_ryan"]\') }}'
  )
}}

{% set models = [
 'chainlink_ethereum_ocr_fulfilled_transactions'
] %}

SELECT *
FROM (
    {% for model in models %}
    SELECT
      blockchain,
      block_time,
      node_address,
      token_amount,
      usd_amount
    FROM {{ ref(model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)