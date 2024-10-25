{{ config(
    schema = 'polymarket_polygon',
    alias = 'market_prices_hourly',
    materialized = 'view',
    unique_key = ['hour', 'token_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.hour')],
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
        LEAD(CAST(date_trunc('hour', block_time) AS timestamp)) OVER (PARTITION BY condition_id, asset_id ORDER BY block_time ASC) AS next_update_hour
    FROM {{ ref('polymarket_polygon_market_trades_raw') }}
    WHERE block_time < DATE_TRUNC('hour', NOW()) 
),

hours AS (
    SELECT distinct date_trunc('hour', block_time) as hour
    FROM {{ source('ethereum', 'transactions') }}
),

latest_prices AS (
    SELECT
        hour,
        condition_id,
        token_id,
        price,
        next_update_hour
    FROM (
        SELECT
            hour,
            condition_id,
            token_id,
            price,
            next_update_hour,
            ROW_NUMBER() OVER (PARTITION BY token_id, hour ORDER BY block_time DESC) AS rn
        FROM changed_prices
    ) t
    WHERE rn = 1
),

forward_fill AS (
    SELECT
        CAST(h.hour AS timestamp) AS hour,
        lp.condition_id,
        lp.token_id,
        lp.price
    FROM hours h
    LEFT JOIN latest_prices lp
        ON h.hour >= lp.hour
        AND (lp.next_update_hour IS NULL OR h.hour < lp.next_update_hour)
),

prices AS (
    SELECT * 
    FROM forward_fill
    WHERE price IS NOT NULL
)

SELECT * FROM prices
{% if is_incremental() %}
WHERE hour >= DATE_TRUNC('hour', NOW() - INTERVAL '2' day)
{% endif %}