{{
  config(
    tags=['dunesql'],
    alias=alias('vrf_reverted_transactions'),
    post_hook='{{ expose_spells(\'["arbitrum","avalanche_c","bnb","ethereum","fantom","polygon"]\',
                            "project",
                            "chainlink",
                            \'["linkpool_jon"]\') }}'
  )
}}

{% set models = [
  'chainlink_arbitrum_vrf_reverted_transactions',
  'chainlink_avalanche_c_vrf_reverted_transactions',
  'chainlink_bnb_vrf_reverted_transactions',
  'chainlink_ethereum_vrf_reverted_transactions',
  'chainlink_fantom_vrf_reverted_transactions',
  'chainlink_polygon_vrf_reverted_transactions'
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