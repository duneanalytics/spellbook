{{
  config(
	  tags=['legacy', 'remove'],
    alias=alias('price_feeds_hourly', legacy_model=True),
    post_hook='{{ expose_spells(\'["bnb","optimism","polygon"]\',
                            "project",
                            "chainlink",
                            \'["msilb7","0xroll","linkpool_ryan"]\') }}'
  )
}}

{% set models = [
  'chainlink_bnb_price_feeds_hourly_legacy',
  'chainlink_optimism_price_feeds_hourly_legacy',
  'chainlink_polygon_price_feeds_hourly_legacy'
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
;