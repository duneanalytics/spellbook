{{
  config(
    schema = 'polymarket_polygon',
    alias = 'positions',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['day', 'address', 'token_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')],
    post_hook = '{{ expose_spells(blockchains = \'["polygon"]\',
                                  spell_type = "project",
                                  spell_name = "polymarket",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

WITH all_prices AS (
    SELECT 
        pp.hour,
        pp.condition_id,
        pp.token_id,
        CASE 
            WHEN md.market_end_time_parsed IS NULL
                OR pp.hour <= md.market_end_time_parsed
            THEN pp.price
            ELSE COALESCE(md.price_modifier, 0)
        END AS price_mod
    FROM {{ ref('polymarket_polygon_market_prices_hourly') }} pp
    LEFT JOIN {{ ref('polymarket_polygon_market_details') }} md ON pp.token_id = md.token_id
),

daily_prices AS (
    SELECT 
        date_trunc('day', hour) AS day,
        token_id,
        MAX_BY(price_mod, hour) AS price
    FROM all_prices
    {% if is_incremental() %}
    WHERE date_trunc('day', hour) > (SELECT MAX(day) FROM {{ this }})
    {% endif %}
    GROUP BY 1, 2
)

SELECT 
    p.day,
    p.address,
    mm.unique_key,
    p.token_id,
    mm.token_outcome,
    mm.token_outcome_name,
    p.balance,
    COALESCE(dp.price, 0) AS price,
    p.balance * COALESCE(dp.price, 0) AS usd_value,
    mm.question_id,
    mm.question AS market_question,
    mm.market_description,
    mm.event_market_name,
    mm.event_market_description,
    mm.active,
    mm.closed,
    mm.accepting_orders,
    mm.polymarket_link,
    mm.market_start_time,
    mm.market_end_time,
    mm.outcome AS market_outcome,
    mm.resolved_on_timestamp
FROM {{ ref('polymarket_polygon_positions_raw') }} p
INNER JOIN {{ ref('polymarket_polygon_market_details') }} mm ON p.token_id = mm.token_id
LEFT JOIN daily_prices dp ON p.token_id = dp.token_id AND p.day = dp.day
WHERE p.day < date_trunc('day', NOW() + interval '2' hour)
{% if is_incremental() %}
AND p.day > (SELECT MAX(day) FROM {{ this }})
{% endif %}

