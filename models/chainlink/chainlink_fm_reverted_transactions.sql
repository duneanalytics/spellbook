{{
  config(
    
    alias='fm_reverted_transactions',
    post_hook='{{ expose_spells(\'["arbitrum","avalanche_c","bnb","ethereum","fantom","gnosis","optimism","polygon"]\',
                            "project",
                            "chainlink",
                            \'["linkpool_jon"]\') }}'
  )
}}

{% set models = [
  'chainlink_arbitrum_fm_reverted_transactions',
  'chainlink_avalanche_c_fm_reverted_transactions',
  'chainlink_bnb_fm_reverted_transactions',
  'chainlink_ethereum_fm_reverted_transactions',
  'chainlink_fantom_fm_reverted_transactions',
  'chainlink_gnosis_fm_reverted_transactions',
  'chainlink_optimism_fm_reverted_transactions',
  'chainlink_polygon_fm_reverted_transactions'
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