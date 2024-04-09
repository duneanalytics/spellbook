{{
  config(
    
    alias='ccip_reverted_transactions',
    post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "base", "bnb", "ethereum", "optimism", "polygon"]\',
                            "project",
                            "chainlink",
                            \'["linkpool_jon"]\') }}'
  )
}}

{% set models = [
  'chainlink_arbitrum_ccip_reverted_transactions',
  'chainlink_avalanche_c_ccip_reverted_transactions',
  'chainlink_base_ccip_reverted_transactions',
  'chainlink_bnb_ccip_reverted_transactions',
  'chainlink_ethereum_ccip_reverted_transactions',
  'chainlink_optimism_ccip_reverted_transactions',
  'chainlink_polygon_ccip_reverted_transactions'
] %}

SELECT *
FROM (
    {% for model in models %}
    SELECT
      blockchain,
      block_time,
      date_start,
      caller_address,
      tx_hash,
      tx_index
    FROM {{ ref(model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)