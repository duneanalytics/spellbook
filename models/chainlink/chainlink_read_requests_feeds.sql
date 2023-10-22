{{
  config(
    tags=['dunesql'],
    alias=alias('chainlink_read_requests_feeds'),
    post_hook='{{ expose_spells(\'["arbitrum","avalanche_c","bnb","ethereum", "optimism","polygon"]\',
                            "project",
                            "chainlink",
                            \'["linkpool_jon"]\') }}'
  )
}}

{% set models = [
  'chainlink_arbitrum_read_requests_feeds',
  'chainlink_avalanche_c_read_requests_feeds',
  'chainlink_bnb_read_requests_feeds',
  'chainlink_ethereum_read_requests_feeds',
  'chainlink_optimism_read_requests_feeds',
  'chainlink_polygon_read_requests_feeds'
] %}

SELECT *
FROM (
    {% for model in models %}
    SELECT
        blockchain,
        date_start,
        feed_address,
        feed_name
    FROM {{ ref(model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)