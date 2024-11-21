{{
  config(
    
    alias='ccip_transmitted_fulfilled',
    post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "base", "bnb", "ethereum", "optimism", "polygon"]\',
                            "project",
                            "chainlink",
                            \'["linkpool_jon"]\') }}'
  )
}}

{% set models = [
  'chainlink_arbitrum_ccip_transmitted_fulfilled',
  'chainlink_avalanche_c_ccip_transmitted_fulfilled',
  'chainlink_base_ccip_transmitted_fulfilled',
  'chainlink_bnb_ccip_transmitted_fulfilled',
  'chainlink_ethereum_ccip_transmitted_fulfilled',
  'chainlink_optimism_ccip_transmitted_fulfilled',
  'chainlink_polygon_ccip_transmitted_fulfilled'
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