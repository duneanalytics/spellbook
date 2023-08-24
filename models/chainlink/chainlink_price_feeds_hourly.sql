{{
  config(
    tags=['dunesql'],
    alias=alias('price_feeds_hourly'),
    post_hook='{{ expose_spells(\'["bnb","optimism","polygon","arbitrum","avalanche_c","ethereum","fantom","gnosis"]\',
                            "project",
                            "chainlink",
                            \'["msilb7","0xroll","linkpool_ryan","linkpool_jon"]\') }}'
  )
}}

{% set models = [
  'chainlink_bnb_price_feeds_hourly',
  'chainlink_optimism_price_feeds_hourly',
  'chainlink_polygon_price_feeds_hourly',
  'chainlink_arbitrum_price_feeds_hourly',
  'chainlink_avalanche_c_price_feeds_hourly',
  'chainlink_ethereum_price_feeds_hourly',
  'chainlink_fantom_price_feeds_hourly',
  'chainlink_gnosis_price_feeds_hourly'
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
      oracle_price_avg,
      underlying_token_price_avg,
      base,
      quote
    FROM {{ ref(model) }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)