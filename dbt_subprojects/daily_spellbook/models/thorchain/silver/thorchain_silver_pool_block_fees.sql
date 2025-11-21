{{ config(
    schema = 'thorchain_silver',
    alias = 'pool_block_fees',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['day', '_unique_key'],
    partition_by = ['day'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')],
    tags = ['thorchain', 'pool_fees', 'silver']
) }}

WITH all_block_id AS (
    SELECT
        cast(date_trunc('day', b.block_timestamp) AS date) AS day,
        pool_name,
        MAX(A._inserted_timestamp) AS _inserted_timestamp
    FROM
        {{ ref('thorchain_silver_block_pool_depths') }} AS a
    JOIN
        {{ ref('thorchain_silver_block_log') }} as b
        ON a.block_timestamp = b.timestamp
    {% if is_incremental() -%}
    WHERE {{ incremental_predicate('b.block_timestamp') }}
    {% endif -%}
    GROUP BY
        cast(date_trunc('day', b.block_timestamp) AS date),
        pool_name
),
total_pool_rewards_tbl AS (
    SELECT
        cast(date_trunc('day', b.block_timestamp) AS date) AS day,
        pool_name,
        SUM(a.rune_e8) AS rewards
    FROM
        {{ ref('thorchain_silver_rewards_event_entries') }} AS a
    JOIN
        {{ ref('thorchain_silver_block_log') }} as b
        ON a.block_timestamp = b.timestamp
    {% if is_incremental() -%}
    WHERE {{ incremental_predicate('b.block_timestamp') }}
    {% endif -%}
    GROUP BY
        cast(date_trunc('day', b.block_timestamp) AS date),
        pool_name
),
total_liquidity_fees_rune_tbl AS (
    SELECT
        cast(date_trunc('day', b.block_timestamp) AS date) AS day,
        pool_name,
        SUM(a.liq_fee_in_rune_e8) AS total_liquidity_fees_rune
    FROM
        {{ ref('thorchain_silver_swap_events') }} AS a
    JOIN {{ ref('thorchain_silver_block_log') }} as b
        ON a.block_timestamp = b.timestamp
    {% if is_incremental() -%}
    WHERE {{ incremental_predicate('b.block_timestamp') }}
    {% endif -%}
    GROUP BY
        cast(date_trunc('day', b.block_timestamp) AS date),
        pool_name
),
liquidity_fees_asset_tbl AS (
    SELECT
        cast(date_trunc('day', block_timestamp) AS date) AS day,
        pool_name,
        SUM(asset_fee) AS assetLiquidityFees
    FROM
    (
        SELECT
            b.block_timestamp,
            pool_name,
            CASE
                WHEN to_asset = 'THOR.RUNE' THEN 0
                ELSE a.liq_fee_e8
            END AS asset_fee
        FROM
            {{ ref('thorchain_silver_swap_events') }} AS a
        JOIN {{ ref('thorchain_silver_block_log') }} as b
            ON a.block_timestamp = b.timestamp
        {% if is_incremental() -%}
        WHERE {{ incremental_predicate('b.block_timestamp') }}
        {% endif -%}
    )
    GROUP BY
        cast(date_trunc('day', block_timestamp) AS date),
        pool_name
),
liquidity_fees_rune_tbl AS (
    SELECT
        cast(date_trunc('day', block_timestamp) AS date) AS day,
        pool_name,
        SUM(asset_fee) AS runeLiquidityFees
    FROM
    (
        SELECT
            b.block_timestamp,
            pool_name,
            CASE
                WHEN to_asset <> 'THOR.RUNE' THEN 0
                ELSE a.liq_fee_e8
            END AS asset_fee
        FROM
            {{ ref('thorchain_silver_swap_events') }} AS a
        JOIN {{ ref('thorchain_silver_block_log') }} as b
            ON a.block_timestamp = b.timestamp
        {% if is_incremental() -%}
        WHERE {{ incremental_predicate('b.block_timestamp') }}
        {% endif -%}
    )
    GROUP BY
        cast(date_trunc('day', block_timestamp) AS date),
        pool_name
)
SELECT
    a.day,
    a.pool_name,
    COALESCE((rewards / power(10, 8)), 0) AS rewards,
    COALESCE((total_liquidity_fees_rune / power(10, 8)), 0) AS total_liquidity_fees_rune,
    COALESCE((assetLiquidityFees / power(10, 8)), 0) AS asset_liquidity_fees,
    COALESCE((runeLiquidityFees / power(10, 8)), 0) AS rune_liquidity_fees,
    (
    (COALESCE(total_liquidity_fees_rune, 0) + COALESCE(rewards, 0)) / power(
        10,
        8
    )
    ) AS earnings,
    concat_ws(
        '-',
        cast(a.day as varchar),
        a.pool_name
    ) AS _unique_key,
    a._inserted_timestamp
FROM
    all_block_id as a
LEFT JOIN total_pool_rewards_tbl as b
    ON a.day = b.day
    AND a.pool_name = b.pool_name
LEFT JOIN total_liquidity_fees_rune_tbl as c
    ON a.day = c.day
    AND a.pool_name = c.pool_name
LEFT JOIN liquidity_fees_asset_tbl as d
    ON a.day = d.day
    AND a.pool_name = d.pool_name
LEFT JOIN liquidity_fees_rune_tbl as e
    ON a.day = e.day
    AND a.pool_name = e.pool_name