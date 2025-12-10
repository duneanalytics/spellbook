{{
  config(
    schema = 'polymarket_polygon',
    alias = 'open_positions',
    materialized = 'view',
    post_hook = '{{ expose_spells(blockchains = \'["polygon"]\',
                                  spell_type = "project",
                                  spell_name = "polymarket",
                                  contributors = \'["hildobby"]\') }}'
  )
}}

WITH latest_day AS (
  SELECT MAX(day) AS day
  FROM {{ ref('polymarket_polygon_positions_raw') }}
  )

, open_positions AS (
    SELECT 
    p.address,
    mm.unique_key,
    p.token_id,
    mm.token_outcome,
    mm.token_outcome_name,
    p.balance,
    mm.question_id,
    mm.question AS market_question,
    mm.market_description,
    mm.event_market_name,
    mm.event_market_description,
    mm.active,
    mm.closed,
    mm.accepting_orders,
    mm.polymarket_link,
    mm.polymarket_link_slug,
    mm.market_start_time,
    mm.market_end_time,
    mm.market_end_time_parsed,
    mm.outcome AS market_outcome,
    mm.resolved_on_timestamp,
    CASE WHEN LOWER(mm.token_outcome)=mm.outcome THEN 1 ELSE 0 END AS modifier
    FROM {{ ref('polymarket_polygon_positions_raw') }} p
    INNER JOIN latest_day ld ON p.day = ld.day
    INNER JOIN {{ ref('polymarket_polygon_market_details') }} mm ON p.token_id = mm.token_id AND mm.market_end_time_parsed > NOW()
    )

SELECT op.address,
op.unique_key,
token_id,
op.token_outcome,
op.token_outcome_name,
op.balance,
op.balance*(CASE WHEN op.market_end_time_parsed IS NULL
  OR p.last_updated <= op.market_end_time_parsed
  THEN p.latest_price
  ELSE COALESCE(modifier, 0)
  END) AS open_interest,
CASE WHEN op.market_end_time_parsed IS NULL
  OR p.last_updated <=op.market_end_time_parsed
  THEN p.latest_price
  ELSE COALESCE(modifier, 0)
  END AS latest_price,
op.question_id,
op.market_question,
op.market_description,
op.event_market_name,
op.event_market_description,
op.active,
op.closed,
op.accepting_orders,
op.polymarket_link,
op.polymarket_link_slug,
op.market_start_time,
op.market_end_time,
op.market_outcome,
op.resolved_on_timestamp
FROM open_positions op
INNER JOIN {{ ref('polymarket_polygon_market_prices_latest') }} p USING (token_id)