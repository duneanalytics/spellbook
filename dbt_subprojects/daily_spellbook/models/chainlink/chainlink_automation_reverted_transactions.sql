{{
  config(
    
    alias='automation_reverted_transactions',
    post_hook='{{ expose_spells(\'["ethereum", "avalanche_c", "bnb", "fantom", "polygon"]\',
                            "project",
                            "chainlink",
                            \'["linkpool_jon"]\') }}'
  )
}}

{% set models = [
  'chainlink_ethereum_automation_reverted_transactions',
  'chainlink_avalanche_c_automation_reverted_transactions',
  'chainlink_bnb_automation_reverted_transactions',
  'chainlink_fantom_automation_reverted_transactions',
  'chainlink_polygon_automation_reverted_transactions'
] %}

SELECT *
FROM (
    {% for model in models %}
    SELECT
      blockchain,
      block_time,
      date_month,
      node_address,
      token_amount,
      usd_amount,
      tx_hash,
      tx_index
    FROM {{ ref(model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)