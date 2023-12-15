{{
  config(
    
    alias='ccip_transmitted_reverted',
    post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "base", "bnb", "ethereum", "optimism", "polygon"]\',
                            "project",
                            "chainlink",
                            \'["linkpool_jon"]\') }}'
  )
}}

{% set models = [
  'chainlink_arbitrum_ccip_transmitted_reverted',
  'chainlink_avalanche_c_ccip_transmitted_reverted',
  'chainlink_base_ccip_transmitted_reverted',
  'chainlink_bnb_ccip_transmitted_reverted',
  'chainlink_ethereum_ccip_transmitted_reverted',
  'chainlink_optimism_ccip_transmitted_reverted',
  'chainlink_polygon_ccip_transmitted_reverted'
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