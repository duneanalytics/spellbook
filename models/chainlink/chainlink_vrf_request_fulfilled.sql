{{
  config(
    tags=['dunesql'],
    alias=alias('request_fulfilled'),
    post_hook='{{ expose_spells(\'["arbitrum","avalanche_c","bnb","ethereum","fantom","polygon"]\',
                            "project",
                            "chainlink",
                            \'["linkpool_jon"]\') }}'
  )
}}

{% set models = [
  'chainlink_arbitrum_vrf_request_fulfilled',
  'chainlink_avalanche_c_vrf_request_fulfilled',
  'chainlink_bnb_vrf_request_fulfilled',
  'chainlink_ethereum_vrf_request_fulfilled',
  'chainlink_fantom_vrf_request_fulfilled',
  'chainlink_polygon_vrf_request_fulfilled'
] %}

SELECT *
FROM (
    {% for model in models %}
    SELECT
      blockchain,
      evt_block_time,
      operator_address,
      token_value       
    FROM {{ ref(model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
