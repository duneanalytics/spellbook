{{
  config(
    schema = 'polymarket_polygon',
    alias = 'positions_raw',
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
)

SELECT 
    day,
    address,
    token_address,
    token_id,
    balance / 1e6 AS balance
FROM balances
{% if is_incremental() %}
WHERE {{ incremental_predicate('day') }}
{% endif %}