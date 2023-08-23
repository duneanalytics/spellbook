{{
  config(
    tags=['dunesql'],
    alias=alias('price_feeds'),
    post_hook='{{ expose_spells(\'["bnb","optimism","polygon","arbitrum","avalanche_c","ethereum","fantom","gnosis"]\',
                            "project",
                            "chainlink",
                            \'["msilb7","0xroll","linkpool_ryan","linkpool_jon]\') }}'
  )
}}

{% set models = [
  'chainlink_bnb_price_feeds',
  'chainlink_optimism_price_feeds',
  'chainlink_polygon_price_feeds',
  'chainlink_arbitrum_price_feeds',
  'chainlink_avalanche_c_price_feeds',
  'chainlink_ethereum_price_feeds',
  'chainlink_fantom_price_feeds',
  'chainlink_gnosis_price_feeds'
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
      underlying_token_price,
      base,
      quote
    FROM {{ ref(model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)