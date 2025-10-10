{{ config(
    schema = 'thorchain_silver',
    alias = 'block_rewards',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['day'],
    partition_by = ['day_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')],
    tags = ['thorchain', 'block_rewards', 'silver']
) }}

-- Complex daily block rewards calculation
WITH all_block_id AS (
    SELECT
        block_timestamp,
        MAX(_inserted_timestamp) AS _inserted_timestamp
    FROM {{ ref('thorchain_silver_block_pool_depths') }}
    WHERE cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '7' day
    GROUP BY block_timestamp
),

avg_nodes_tbl AS (
    SELECT
        a.block_timestamp,
        SUM(
            CASE
                WHEN a.current_status = 'Active' THEN 1
                WHEN a.former_status = 'Active' THEN -1
                ELSE 0
            END
        ) AS delta
    FROM {{ ref('thorchain_silver_update_node_account_status_events') }} a  -- ✅ Now converted!
    WHERE cast(from_unixtime(cast(a.block_timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '7' day
    GROUP BY a.block_timestamp
),

all_block_with_nodes AS (
    SELECT
        abi.block_timestamp,
        COALESCE(ant.delta, 0) AS delta,
        SUM(COALESCE(ant.delta, 0)) OVER (
            ORDER BY abi.block_timestamp ASC
        ) AS avg_nodes,
        abi._inserted_timestamp
    FROM all_block_id abi
    LEFT JOIN avg_nodes_tbl ant
        ON abi.block_timestamp = ant.block_timestamp
),

all_block_with_nodes_date AS (
    SELECT
        DATE(cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp)) AS day,
        AVG(abwn.avg_nodes) AS avg_nodes,
        MAX(abwn._inserted_timestamp) AS _inserted_timestamp
    FROM all_block_with_nodes abwn
    JOIN {{ source('thorchain', 'block_log') }} b
        ON abwn.block_timestamp = b.timestamp
    WHERE cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '7' day
    {% if is_incremental() %}
      AND {{ incremental_predicate('DATE(cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp))') }}
    {% endif %}
    GROUP BY DATE(cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp))
),

liquidity_fee_tbl AS (
    SELECT
        DATE(cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp)) AS day,
        COALESCE(SUM(a.liq_fee_in_rune_e8), 0) AS liquidity_fee
    FROM {{ ref('thorchain_silver_swap_events') }} a
    JOIN {{ source('thorchain', 'block_log') }} b
        ON a.raw_block_timestamp = b.timestamp
    WHERE cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp) >= current_date - interval '7' day
    {% if is_incremental() %}
      AND {{ incremental_predicate('DATE(cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp))') }}
    {% endif %}
    GROUP BY DATE(cast(from_unixtime(cast(b.timestamp / 1e9 as bigint)) as timestamp))
),

-- CORRECT LOGIC: Use the total_block_rewards aggregation instead of duplicating
rewards_summary AS (
    SELECT
        date(tbr.block_time) AS day,
        SUM(
            CASE 
                WHEN tbr.reward_entity = 'bond_holders' THEN tbr.rune_amount
                ELSE 0
            END
        ) AS bond_earnings,
        SUM(
            CASE 
                WHEN tbr.reward_entity != 'bond_holders' THEN tbr.rune_amount
                ELSE 0
            END
        ) AS total_pool_rewards
    FROM {{ ref('thorchain_silver_total_block_rewards') }} tbr
    WHERE tbr.block_time >= current_date - interval '7' day
    {% if is_incremental() %}
      AND {{ incremental_predicate('date(tbr.block_time)') }}
    {% endif %}
    GROUP BY date(tbr.block_time)
),

base AS (
    SELECT
        abwnd.day,
        date_trunc('month', abwnd.day) as day_month,
        COALESCE((lft.liquidity_fee / power(10, 8)), 0) AS liquidity_fee,
        
        -- Block rewards calculation (using total_block_rewards aggregation)
        (COALESCE(rs.total_pool_rewards, 0) + COALESCE(rs.bond_earnings, 0)) AS block_rewards,
        
        -- Total earnings calculation
        (COALESCE(rs.total_pool_rewards, 0) + COALESCE(lft.liquidity_fee / power(10, 8), 0) + COALESCE(rs.bond_earnings, 0)) AS earnings,
        
        COALESCE(rs.bond_earnings, 0) AS bonding_earnings,
        
        -- Liquidity earnings calculation  
        (COALESCE(rs.total_pool_rewards, 0) + COALESCE(lft.liquidity_fee / power(10, 8), 0)) AS liquidity_earnings,
        
        abwnd.avg_nodes + 2 AS avg_node_count,
        abwnd._inserted_timestamp
        
    FROM all_block_with_nodes_date abwnd
    LEFT JOIN liquidity_fee_tbl lft
        ON abwnd.day = lft.day
    LEFT JOIN rewards_summary rs
        ON abwnd.day = rs.day
)

SELECT * FROM base
{% if is_incremental() %}
WHERE {{ incremental_predicate('base.day') }}
{% endif %}
