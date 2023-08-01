{{
  config(
    tags=['dunesql'],
    alias=alias('price_feeds_hourly'),
    post_hook='{{ expose_spells(\'["bnb","optimism","polygon"]\',
                            "project",
                            "chainlink",
                            \'["msilb7","0xroll","linkpool_ryan"]\') }}'
  )
}}

{% set models = [
  'chainlink_bnb_price_feeds_hourly',
  'chainlink_optimism_price_feeds_hourly',
  'chainlink_polygon_price_feeds_hourly'
] %}

SELECT *
FROM (
    {% for model in models %}
    SELECT
      blockchain,
      hour,
      block_date,
      feed_name,
      proxy_address,
      aggregator_address,
      underlying_token_address, 
      oracle_price_avg,
      underlying_token_price_avg
    FROM {{ ref(model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)