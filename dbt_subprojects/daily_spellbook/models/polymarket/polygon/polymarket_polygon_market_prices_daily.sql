{{ config(
    schema = 'polymarket_polygon',
    alias = 'market_prices_daily',
    materialized = 'view',
    unique_key = ['day', 'token_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')],
    post_hook = '{{ expose_spells(blockchains = \'["polygon"]\',
                                  spell_type = "project",
                                  spell_name = "polymarket",
                                  contributors = \'["0xboxer"]\') }}'
  )
}}

WITH changed_prices AS (
    SELECT
        date_trunc('day', block_time) AS day,
        block_time,
        condition_id,
        asset_id AS token_id,
        price,
        LEAD(CAST(date_trunc('day', block_time) AS timestamp)) OVER (PARTITION BY condition_id, asset_id ORDER BY block_time ASC) AS next_update_day
    FROM {{ ref('polymarket_polygon_market_trades_raw') }}
    WHERE block_time < DATE_TRUNC('day', NOW()) 
),

--sequences are limited to 10k so just pulling this in from the transactions table, no other relationship
days AS (
    Select date_trunc('day', block_time) as day
    from {{ source('ethereum', 'transactions') }}
),

latest_prices AS (
    SELECT
        day,
        condition_id,
        token_id,
        price,
        next_update_day
    FROM (
        SELECT
            day,
            condition_id,
            token_id,
            price,
            next_update_day,
            ROW_NUMBER() OVER (PARTITION BY token_id, day ORDER BY block_time DESC) AS rn
        FROM changed_prices
    ) t
    WHERE rn = 1
),

forward_fill AS (
    SELECT
        CAST(d.day AS timestamp) AS day,
        lp.condition_id,
        lp.token_id,
        lp.price
    FROM days d
    LEFT JOIN latest_prices lp
        ON d.day >= lp.day
        AND (lp.next_update_day IS NULL OR d.day < lp.next_update_day)
),

prices AS (
    SELECT * 
    FROM forward_fill
    WHERE price IS NOT NULL
)

SELECT * FROM prices
{% if is_incremental() %}
WHERE day >= DATE_TRUNC('day', NOW() - INTERVAL '2' day)
{% endif %}