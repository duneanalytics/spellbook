{{
  config(
    tags=['dunesql'],
    alias=alias('price_feeds'),
    post_hook='{{ expose_spells(\'["bnb","optimism","polygon"]\',
                            "project",
                            "chainlink",
                            \'["msilb7","0xroll","linkpool_ryan"]\') }}'
  )
}}

{% set models = [
  'chainlink_bnb_price_feeds',
  'chainlink_optimism_price_feeds',
  'chainlink_polygon_price_feeds'
] %}

SELECT *
FROM (
    {% for model in models %}
    SELECT
      blockchain,
      block_time,
      block_date,
      block_number,
      feed_name,
      oracle_price,
      proxy_address,
      aggregator_address,
      underlying_token_address,
      underlying_token_price
    FROM {{ ref(model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)