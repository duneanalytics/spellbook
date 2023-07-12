{{ config(alias='app_data',
        tags=['dunesql'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "cow_protocol",
                                    \'["bh2smith"]\') }}'
)}}

-- Find the PoC Query here: https://dune.com/queries/1751965
with
  partially_unpacked_app_content AS (
    SELECT
      app_hash,
      content.appCode AS app_code,
      content.environment,
      content.metadata.orderClass.orderClass AS order_class,
      content.metadata.quote,
      content.metadata.referrer,
      content.metadata.utm
    FROM {{ source('cowswap', 'raw_app_data') }}
  ),
  unpacked_referrer_app_data AS (
    SELECT
      app_hash,
      app_code,
      environment,
      order_class,
      quote,
      LOWER(COALESCE(referrer.address, referrer.referrer)) AS referrer,
      utm
    FROM
      partially_unpacked_app_content
  ),
  results AS (
    SELECT
      app_hash,
      app_code,
      environment,
      order_class,
      referrer,
      TRY_CAST(quote.slippageBips AS INTEGER) AS slippage_bips,
      utm
    FROM
      unpacked_referrer_app_data
  )
SELECT * FROM results
