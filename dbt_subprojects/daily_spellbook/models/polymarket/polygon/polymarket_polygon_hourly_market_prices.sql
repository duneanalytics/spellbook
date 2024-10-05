{{
  config(
    schema = 'polymarket_polygon',
    alias = 'hourly_market_prices',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['hour', 'condition_id', 'token_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.hour')],
    post_hook = '{{ expose_spells(blockchains = \'["polygon"]\',
                                  spell_type = "project",
                                  spell_name = "polymarket",
                                  contributors = \'["your_name_here"]\') }}'
  )
}}

WITH changed_prices AS (
    SELECT
        date_trunc('hour', block_time) AS hour,
        condition_id,
        asset_id AS token_id,
        price,
        LEAD(CAST(date_trunc('hour', block_time) AS timestamp)) OVER (PARTITION BY condition_id, asset_id ORDER BY block_time ASC) AS next_update_hour
    FROM {{ ref('polymarket_polygon_raw_market_trades') }}
    WHERE block_time < DATE_TRUNC('hour', NOW()) 
),

hours AS (
    SELECT *
    FROM UNNEST(
        SEQUENCE(CAST('2021-03-16' AS timestamp), DATE_TRUNC('hour', NOW()), INTERVAL '1' hour)
    ) AS foo(hour)
),

forward_fill AS (
    SELECT
        CAST(h.hour AS timestamp) AS hour,
        cp.condition_id,
        cp.token_id,
        cp.price
    FROM hours h
    LEFT JOIN changed_prices cp
        ON h.hour >= cp.hour
        AND (cp.next_update_hour IS NULL OR h.hour < cp.next_update_hour)
),

prices AS (
    SELECT * 
    FROM forward_fill
    WHERE price IS NOT NULL
)

SELECT * FROM prices
{% if is_incremental() %}
WHERE hour >= DATE_TRUNC('day', NOW() - INTERVAL '7' day)
{% endif %}