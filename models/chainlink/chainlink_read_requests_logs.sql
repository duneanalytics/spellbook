{{
  config(
    tags=['dunesql'],
    alias=alias('chainlink_read_requests_logs'),
    post_hook='{{ expose_spells(\'["arbitrum","avalanche_c","bnb","ethereum", "optimism","polygon"]\',
                            "project",
                            "chainlink",
                            \'["linkpool_jon"]\') }}'
  )
}}

{% set models = [
  'chainlink_arbitrum_read_requests_logs',
  'chainlink_avalanche_c_read_requests_logs',
  'chainlink_bnb_read_requests_logs',
  'chainlink_ethereum_read_requests_logs',
  'chainlink_optimism_read_requests_logs',
  'chainlink_polygon_read_requests_logs'
] %}

SELECT *
FROM (
    {% for model in models %}
    SELECT
        blockchain,
        block_hash,
        block_number,
        block_time,
        tx_hash,
        "from",
        "to",
        input,
        "output"
    FROM {{ ref(model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)