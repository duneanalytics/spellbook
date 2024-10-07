{{
  config(
    schema = 'polymarket_polygon',
    alias = 'positions',
    materialized = 'view',
    post_hook = '{{ expose_spells(blockchains = \'["polygon"]\',
                                  spell_type = "project",
                                  spell_name = "polymarket",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

SELECT 
    p.day,
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
    mm.market_start_time,
    mm.market_end_time,
    mm.outcome AS market_outcome,
    mm.resolved_on_timestamp
FROM {{ ref('polymarket_polygon_positions_raw') }} p
INNER JOIN {{ ref('polymarket_polygon_market_details') }} mm ON p.token_id = mm.token_id

