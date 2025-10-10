{{ config(
    schema = 'thorchain_silver',
    alias = 'total_block_rewards',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['_unique_key'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'total_block_rewards', 'silver']
) }}

-- FOLLOWING ORIGINAL FLIPSIDE LOGIC: Combine pool rewards + bond earnings
WITH block_prices AS (
    SELECT
        COALESCE(AVG(p.price), 0) AS rune_usd,
        p.block_height AS block_id  -- Using block_height from prices model
    FROM {{ ref('thorchain_silver_prices') }} p
    WHERE p.symbol = 'RUNE'
      AND p.block_time >= current_date - interval '7' day
    GROUP BY p.block_height
),

-- SECTION 1: Pool rewards (per pool) - ORIGINAL FLIPSIDE LOGIC
pool_rewards AS (
    SELECT
        b.block_timestamp,
        cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp) as block_time,
        date(from_unixtime(cast(b.timestamp / 1e9 as bigint))) as block_date,
        date_trunc('month', from_unixtime(cast(b.timestamp / 1e9 as bigint))) as block_month,
        b.height AS block_id,
        ree.pool_name AS reward_entity,
        COALESCE(ree.rune_e8 / pow(10, 8), 0) AS rune_amount,
        COALESCE(ree.rune_e8 / pow(10, 8) * COALESCE(bp.rune_usd, 0), 0) AS rune_amount_usd,
        concat(
            cast(b.height as varchar),
            '-',
            ree.pool_name
        ) AS _unique_key,
        ree._inserted_timestamp
    FROM {{ ref('thorchain_silver_rewards_event_entries') }} ree
    JOIN {{ source('thorchain', 'block_log') }} b
        ON ree.block_timestamp = b.timestamp
    LEFT JOIN block_prices bp
        ON b.height = bp.block_id
    WHERE cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '7' day
    {% if is_incremental() %}
      AND {{ incremental_predicate('cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp)') }}
    {% endif %}
),

-- SECTION 2: Bond holder rewards (using bond_events as source) - ADAPTED FLIPSIDE LOGIC
bond_rewards AS (
    SELECT
        b.block_timestamp,
        cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp) as block_time,
        date(from_unixtime(cast(b.timestamp / 1e9 as bigint))) as block_date,
        date_trunc('month', from_unixtime(cast(b.timestamp / 1e9 as bigint))) as block_month,
        b.height AS block_id,
        'bond_holders' AS reward_entity,  -- SAME AS FLIPSIDE LOGIC
        be.e8 / pow(10, 8) AS rune_amount,  -- Using bond_events.e8 instead of rewards_events.bond_e8
        be.e8 / pow(10, 8) * COALESCE(bp.rune_usd, 0) AS rune_amount_usd,
        concat(
            cast(b.height as varchar),
            '-',
            'bond_holders'
        ) AS _unique_key,
        be._inserted_timestamp
    FROM {{ ref('thorchain_silver_bond_events') }} be
    JOIN {{ source('thorchain', 'block_log') }} b
        ON be.block_timestamp = b.timestamp
    LEFT JOIN block_prices bp
        ON b.height = bp.block_id
    WHERE cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '7' day
      AND be.bond_type IN ('bond_reward', 'reward')  -- Filter for earnings-type bonds
    {% if is_incremental() %}
      AND {{ incremental_predicate('cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp)') }}
    {% endif %}
),

-- UNION LOGIC - EXACTLY AS FLIPSIDE HAD IT
combined_rewards AS (
    SELECT * FROM pool_rewards
    UNION ALL
    SELECT * FROM bond_rewards
),

-- FINAL AGGREGATION - SAME AS FLIPSIDE
base AS (
    SELECT
        block_timestamp,
        block_time,
        block_date,
        block_month,
        block_id,
        reward_entity,
        SUM(rune_amount) AS rune_amount,
        SUM(rune_amount_usd) AS rune_amount_usd,
        _unique_key,
        MAX(_inserted_timestamp) AS _inserted_timestamp
    FROM combined_rewards
    GROUP BY
        block_timestamp,
        block_time,
        block_date,
        block_month,
        block_id,
        reward_entity,
        _unique_key
)

SELECT * FROM base
{% if is_incremental() %}
WHERE {{ incremental_predicate('base.block_time') }}
{% endif %}
