{{
  config(
    tags=['dunesql'],
    alias=alias('vrf_gas_daily'),
    post_hook='{{ expose_spells(\'["arbitrum","avalanche_c","bnb","ethereum","fantom","polygon"]\',
                            "project",
                            "chainlink",
                            \'["linkpool_jon"]\') }}'
  )
}}

{% set models = [
  'chainlink_arbitrum_vrf_gas_daily',
  'chainlink_avalanche_c_vrf_gas_daily',
  'chainlink_bnb_vrf_gas_daily',
  'chainlink_ethereum_vrf_gas_daily',
  'chainlink_fantom_vrf_gas_daily',
  'chainlink_polygon_vrf_gas_daily'
] %}

SELECT *
FROM (
    {% for model in models %}
    SELECT
      blockchain,
      date_start,
      date_month,
      node_address,
      fulfilled_token_amount,
      fulfilled_usd_amount,
      reverted_token_amount,
      reverted_usd_amount,
      total_token_amount,
      total_usd_amount        
    FROM {{ ref(model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)