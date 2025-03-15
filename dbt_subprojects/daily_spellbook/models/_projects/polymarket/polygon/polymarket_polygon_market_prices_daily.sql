{{ config(
    schema = 'polymarket_polygon',
    alias = 'market_prices_daily',
    materialized = 'view',
    tags = ['prod_exclude']
    )
}}

WITH changed_prices AS (
    SELECT
        date_trunc('day', block_time) AS day,
        block_time,
        condition_id,
        asset_id AS token_id,
        price,
        LEAD(CAST(date_trunc('day', block_time) AS timestamp)) OVER (PARTITION BY asset_id ORDER BY block_time ASC) AS next_update_day
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('day', block_time), asset_id ORDER BY block_time DESC) as rn
        FROM {{ ref('polymarket_polygon_market_trades_raw') }}
    ) ranked
    WHERE rn = 1
),

--sequences are limited to 10k so just pulling this in from the transactions table, no other relationship
days AS (
    SELECT *
    FROM UNNEST(
        SEQUENCE(CAST('2015-01-01' AS date), DATE(DATE_TRUNC('day', NOW())), INTERVAL '1' day)
    ) AS foo(day)
),

forward_fill AS (
    SELECT
        CAST(d.day AS timestamp) AS day,
        lp.condition_id,
        lp.token_id,
        lp.price
    FROM days d
    LEFT JOIN changed_prices lp
        ON d.day >= lp.day
        AND (lp.next_update_day IS NULL OR d.day < lp.next_update_day)
),

-- Join with market details to get market end time and token outcome information
price_correction AS (
    SELECT
        ff.day,
        ff.condition_id,
        ff.token_id,
        CASE 
            WHEN ff.day <= TRY_CAST(SUBSTRING(md.market_end_time FROM 1 FOR 19) AS timestamp) THEN ff.price
            WHEN ff.day > TRY_CAST(SUBSTRING(md.market_end_time FROM 1 FOR 19) AS timestamp) THEN
                CASE 
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

SELECT * FROM price_correction
WHERE price > 0
