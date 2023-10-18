{{
  config(
    tags=['dunesql'],
    alias=alias('vrf_fulfilled_transactions'),
    post_hook='{{ expose_spells(\'["arbitrum","avalanche_c","bnb","ethereum","fantom","polygon"]\',
                            "project",
                            "chainlink",
                            \'["linkpool_jon"]\') }}'
  )
}}

{% set models = [
  'chainlink_arbitrum_vrf_fulfilled_transactions',
  'chainlink_avalanche_c_vrf_fulfilled_transactions',
  'chainlink_bnb_vrf_fulfilled_transactions',
  'chainlink_ethereum_vrf_fulfilled_transactions',
  'chainlink_fantom_vrf_fulfilled_transactions',
  'chainlink_polygon_vrf_fulfilled_transactions'
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