{{ config(
    schema = 'polymarket_polygon',
    alias = 'market_prices_hourly',
    materialized = 'view',
    post_hook = '{{ expose_spells(blockchains = \'["polygon"]\',
                                  spell_type = "project",
                                  spell_name = "polymarket",
                                  contributors = \'["0xboxer, tomfutago"]\') }}'
  )
}}

WITH changed_prices AS (
    SELECT
        date_trunc('hour', block_time) AS hour,
        block_time,
        condition_id,
        asset_id AS token_id,
        price,
        LEAD(CAST(date_trunc('hour', block_time) AS timestamp)) OVER (PARTITION BY asset_id ORDER BY block_time ASC) AS next_update_hour
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('hour', block_time), asset_id ORDER BY block_time DESC) as rn
        FROM {{ ref('polymarket_polygon_market_trades_raw') }}
    ) ranked
    WHERE rn = 1
),

hours AS (
   Select distinct date_trunc('hour', block_time) as hour
   from {{ source('polygon', 'transactions') }}
),

forward_fill AS (
    SELECT
        CAST(h.hour AS timestamp) AS hour,
        lp.condition_id,
        lp.token_id,
        lp.price
    FROM hours h
    LEFT JOIN changed_prices lp
        ON h.hour >= lp.hour
        AND (lp.next_update_hour IS NULL OR h.hour < lp.next_update_hour)
)

SELECT * FROM forward_fill
WHERE price > 0
