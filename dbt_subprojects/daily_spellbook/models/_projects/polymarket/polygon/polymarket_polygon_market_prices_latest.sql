{{ config(
    schema = 'polymarket_polygon',
    alias = 'market_prices_latest',
    materialized = 'view',
    unique_key = ['token_id'],
    post_hook = '{{ expose_spells(blockchains = \'["polygon"]\',
                                  spell_type = "project",
                                  spell_name = "polymarket",
                                  contributors = \'["0xboxer, tomfutago"]\') }}'
  )
}}

WITH latest_prices AS (
    SELECT
        block_time as last_updated,
        condition_id,
        asset_id as token_id,
        price as latest_price,
        ROW_NUMBER() OVER (PARTITION BY asset_id ORDER BY block_time DESC) as rn
    FROM {{ ref('polymarket_polygon_market_trades_raw') }}
)

SELECT 
    last_updated,
    condition_id,
    token_id,
    latest_price
FROM latest_prices
WHERE rn = 1
  AND latest_price > 0
