{{
  config(
    schema = 'polymarket_polygon',
    alias = 'positions',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['address', 'token_address', 'token_id', 'day'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    post_hook = '{{ expose_spells(blockchains = \'["polygon"]\',
                                  spell_type = "project",
                                  spell_name = "polymarket",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

WITH changed_balances AS (
    SELECT
        blockchain,
        day,
        address,
        token_symbol,
        token_address,
        token_standard,
        token_id,
        balance,
        LEAD(CAST(day AS timestamp)) OVER (PARTITION BY token_address, address, token_id ORDER BY block_time ASC) AS next_update_day
    FROM {{ source('tokens_polygon', 'balances_daily_agg') }}
    WHERE day < DATE(DATE_TRUNC('day', NOW())) 
      AND token_standard = 'erc1155' 
      AND token_address = 0x4D97DCd97eC945f40cF65F87097ACe5EA0476045
),

days AS (
    SELECT *
    FROM UNNEST(
        SEQUENCE(CAST('2015-01-01' AS date), DATE(DATE_TRUNC('day', NOW())), INTERVAL '1' day)
    ) AS foo(day)
),

forward_fill AS (
    SELECT
        blockchain,
        CAST(d.day AS timestamp) AS day,
        address,
        token_symbol,
        token_address,
        token_standard,
        token_id,
        balance
    FROM days d
    LEFT JOIN changed_balances b
        ON d.day >= b.day
        AND (b.next_update_day IS NULL OR d.day < b.next_update_day)
),

balances AS (
    SELECT * 
    FROM forward_fill
    WHERE balance > 0
),

markets_mapping AS (
    SELECT * 
    FROM {{ ref('polymarket_polygon_market_details') }}
),

mapped_balances AS (
    SELECT 
        day,
        address,
        mm.unique_key,
        b.token_id,
        mm.token_outcome,
        mm.token_outcome_name,
        balance / 1e6 AS balance,
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
    FROM balances b
    INNER JOIN markets_mapping mm ON b.token_id = mm.token_id
    {% if is_incremental() %}
    where {{ incremental_predicate('b.day') }}
    {% endif %}
)

SELECT * FROM mapped_balances

