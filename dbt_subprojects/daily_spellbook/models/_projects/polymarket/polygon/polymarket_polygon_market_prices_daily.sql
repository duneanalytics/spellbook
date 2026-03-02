{{ config(
    schema = 'polymarket_polygon',
    alias = 'market_prices_daily',
    materialized = 'view'
    )
}}

WITH changed_prices AS (
    SELECT date_trunc('day', block_time) AS day
    , block_time
    , condition_id
    , token_id
    , price
    , LEAD(date_trunc('day', block_time)) OVER (PARTITION BY token_id ORDER BY block_time ASC) AS next_update_day
    FROM (
        SELECT block_time
        , condition_id
        , asset_id AS token_id
        , price
        , ROW_NUMBER() OVER (PARTITION BY date_trunc('day', block_time), asset_id ORDER BY block_time DESC) AS rn
        FROM {{ ref('polymarket_polygon_market_trades_raw') }}
        ) ranked
    WHERE rn = 1
    )

, days AS (
    SELECT timestamp AS day
    FROM {{ ref('utils_days') }}
    WHERE timestamp >= date('2022-11-21')
    )

, forward_fill AS (
    SELECT CAST(d.day AS timestamp) AS hour
    , lp.condition_id
    , lp.token_id
    , lp.price
    FROM days d
    LEFT JOIN changed_prices lp
        ON d.day >= lp.day
        AND (lp.next_update_day IS NULL OR d.day < lp.next_update_day)
    )

, price_correction AS (
    SELECT ff.day
    , ff.condition_id
    , ff.token_id
    , CASE WHEN ff.hour <= md.market_end_time_parsed THEN ff.price
        WHEN ff.hour > md.market_end_time_parsed
            THEN CASE 
                WHEN md.token_outcome = 'Yes' AND md.outcome = 'yes' THEN 1
                WHEN md.token_outcome = 'Yes' AND md.outcome = 'no' THEN 0
                WHEN md.token_outcome = 'No' AND md.outcome = 'yes' THEN 0
                WHEN md.token_outcome = 'No' AND md.outcome = 'no' THEN 1
                ELSE ff.price
                END
            ELSE ff.price
        END AS price
    FROM forward_fill ff
    LEFT JOIN {{ ref('polymarket_polygon_market_details') }} md ON ff.token_id = md.token_id
    )

SELECT day
, condition_id
, token_id
, price
FROM price_correction
WHERE price > 0