{{
  config(
    
    alias='automation_upkeep_performed_logs',
    post_hook='{{ expose_spells(\'["ethereum", "avalanche_c", "bnb", "fantom", "polygon"]\',
                            "project",
                            "chainlink",
                            \'["linkpool_jon"]\') }}'
  )
}}

{% set models = [
  'chainlink_ethereum_automation_upkeep_performed_logs',
  'chainlink_avalanche_c_automation_upkeep_performed_logs',
  'chainlink_bnb_automation_upkeep_performed_logs',
  'chainlink_fantom_automation_upkeep_performed_logs',
  'chainlink_polygon_automation_upkeep_performed_logs'
] %}

SELECT *
FROM (
    {% for model in models %}
    SELECT
      blockchain,
      block_hash,
      contract_address,
      data,
      topic0,
      topic1,
      topic2,
      topic3,
      tx_hash,
      block_number,
      block_time,
      index,
      tx_index
    FROM {{ ref(model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)