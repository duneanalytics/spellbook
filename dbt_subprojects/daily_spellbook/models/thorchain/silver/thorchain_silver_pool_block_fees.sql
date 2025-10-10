{{ config(
    schema = 'thorchain_silver',
    alias = 'pool_block_fees',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['_unique_key'],
    partition_by = ['day_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day_month')],
    tags = ['thorchain', 'pool_fees', 'silver']
) }}

-- CRITICAL: Use CTE pattern for complex daily aggregation
WITH all_block_id AS (
    SELECT
        date(b.block_time) AS day,
        a.pool_name,
        MAX(a._inserted_timestamp) AS _inserted_timestamp
    FROM {{ ref('thorchain_silver_block_pool_depths') }} a
    JOIN {{ ref('thorchain_core_dim_block') }} b
        ON a.block_time = b.block_time
    WHERE b.block_time >= current_date - interval '7' day
    {% if is_incremental() %}
      AND {{ incremental_predicate('b.block_time') }}
    {% endif %}
    GROUP BY
        date(b.block_time),
        a.pool_name
),

total_pool_rewards_tbl AS (
    SELECT
        date(b.block_time) AS day,
        a.pool_name,
        SUM(a.rune_e8) AS rewards
    FROM {{ ref('thorchain_silver_rewards_event_entries') }} a
    JOIN {{ ref('thorchain_core_dim_block') }} b
        ON a.block_time = b.block_time
    WHERE b.block_time >= current_date - interval '7' day
    {% if is_incremental() %}
      AND {{ incremental_predicate('b.block_time') }}
    {% endif %}
    GROUP BY
        date(b.block_time),
        a.pool_name
),

total_liquidity_fees_rune_tbl AS (
    SELECT
        date(a.block_time) AS day,
        a.pool AS pool_name,  -- FIXED: Use correct column name
        SUM(a.liq_fee_in_rune_e8) AS total_liquidity_fees_rune
    FROM {{ ref('thorchain_silver_swap_events') }} a
    WHERE a.block_time >= current_date - interval '7' day  -- FIXED: Use existing block_time
    {% if is_incremental() %}
      AND {{ incremental_predicate('a.block_time') }}
    {% endif %}
    GROUP BY
        date(a.block_time),
        a.pool
),

liquidity_fees_asset_tbl AS (
    SELECT
        date(block_time) AS day,
        pool AS pool_name,  -- FIXED: Use correct column name
        SUM(asset_fee) AS asset_liquidity_fees
    FROM (
        SELECT
            a.block_time,
            a.pool,
            CASE
                WHEN a.to_asset = 'THOR.RUNE' THEN 0
                ELSE a.liq_fee_e8
            END AS asset_fee
        FROM {{ ref('thorchain_silver_swap_events') }} a
        WHERE a.block_time >= current_date - interval '7' day  -- FIXED: Use existing block_time
        {% if is_incremental() %}
          AND {{ incremental_predicate('a.block_time') }}
        {% endif %}
    )
    GROUP BY
        date(block_time),
        pool
),

liquidity_fees_rune_tbl AS (
    SELECT
        date(block_time) AS day,
        pool AS pool_name,  -- FIXED: Use correct column name
        SUM(asset_fee) AS rune_liquidity_fees
    FROM (
        SELECT
            a.block_time,
            a.pool,
            CASE
                WHEN a.to_asset <> 'THOR.RUNE' THEN 0
                ELSE a.liq_fee_e8
            END AS asset_fee
        FROM {{ ref('thorchain_silver_swap_events') }} a
        WHERE a.block_time >= current_date - interval '7' day  -- FIXED: Use existing block_time
        {% if is_incremental() %}
          AND {{ incremental_predicate('a.block_time') }}
        {% endif %}
    )
    GROUP BY
        date(block_time),
        pool
),

base AS (
    SELECT
        a.day,
        a.pool_name,
        date_trunc('month', cast(a.day as timestamp)) as day_month,
        COALESCE((b.rewards / power(10, 8)), 0) AS rewards,
        COALESCE((c.total_liquidity_fees_rune / power(10, 8)), 0) AS total_liquidity_fees_rune,
        COALESCE((d.asset_liquidity_fees / power(10, 8)), 0) AS asset_liquidity_fees,
        COALESCE((e.rune_liquidity_fees / power(10, 8)), 0) AS rune_liquidity_fees,
        ((COALESCE(c.total_liquidity_fees_rune, 0) + COALESCE(b.rewards, 0)) / power(10, 8)) AS earnings,
        concat(
            cast(a.day as varchar),
            '-',
            a.pool_name
        ) AS _unique_key,
        a._inserted_timestamp
    FROM all_block_id a
    LEFT JOIN total_pool_rewards_tbl b
        ON a.day = b.day
        AND a.pool_name = b.pool_name
    LEFT JOIN total_liquidity_fees_rune_tbl c
        ON a.day = c.day
        AND a.pool_name = c.pool_name  -- FIXED: Now using pool_name consistently
    LEFT JOIN liquidity_fees_asset_tbl d
        ON a.day = d.day
        AND a.pool_name = d.pool_name  -- FIXED: Now using pool_name consistently
    LEFT JOIN liquidity_fees_rune_tbl e
        ON a.day = e.day
        AND a.pool_name = e.pool_name  -- FIXED: Now using pool_name consistently
)

SELECT * FROM base
{% if is_incremental() %}
WHERE {{ incremental_predicate('base.day_month') }}
{% endif %}
