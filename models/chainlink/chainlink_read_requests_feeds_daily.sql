{{
  config(
    
    alias='chainlink_read_requests_feeds_daily',
    post_hook='{{ expose_spells(\'["arbitrum","avalanche_c","bnb","ethereum", "optimism","polygon"]\',
                            "project",
                            "chainlink",
                            \'["linkpool_jon"]\') }}'
  )
}}

{% set models = [
  'chainlink_arbitrum_read_requests_feeds_daily',
  'chainlink_avalanche_c_read_requests_feeds_daily',
  'chainlink_bnb_read_requests_feeds_daily',
  'chainlink_ethereum_read_requests_feeds_daily',
  'chainlink_optimism_read_requests_feeds_daily',
  'chainlink_polygon_read_requests_feeds_daily'
] %}

SELECT *
FROM (
    {% for model in models %}
    SELECT
        blockchain,
        date_month,
        date_start,
        feed_address,
        feed_name,
        total_requests
    FROM {{ ref(model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)