{{ config(
    schema = 'thorchain_silver',
    alias = 'block_rewards',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = 'block_date',
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    tags = ['thorchain', 'block_rewards', 'silver']
) }}

WITH all_block_id AS (
    SELECT
        block_time,
        MAX(_inserted_timestamp) AS _inserted_timestamp
    FROM {{ ref('thorchain_silver_block_pool_depths') }}
    WHERE block_time >= current_date - interval '17' day
    GROUP BY block_time
),

avg_nodes_tbl AS (
    SELECT
        a.block_time,
        SUM(
            CASE
                WHEN a.current_status = 'Active' THEN 1
                WHEN a.former_status = 'Active' THEN -1
                ELSE 0
            END
        ) AS delta
    FROM {{ ref('thorchain_silver_update_node_account_status_events') }} a
    WHERE a.block_time >= current_date - interval '17' day
    GROUP BY a.block_time
),

all_block_with_nodes AS (
    SELECT
        abi.block_time,
        COALESCE(ant.delta, 0) AS delta,
        SUM(COALESCE(ant.delta, 0)) OVER (
            ORDER BY abi.block_time ASC
        ) AS avg_nodes,
        abi._inserted_timestamp
    FROM all_block_id abi
    LEFT JOIN avg_nodes_tbl ant
        ON abi.block_time = ant.block_time
),

all_block_with_nodes_date AS (
    SELECT
        DATE(abwn.block_time) AS block_date,
        AVG(abwn.avg_nodes) AS avg_nodes,
        MAX(abwn._inserted_timestamp) AS _inserted_timestamp
    FROM all_block_with_nodes abwn
    WHERE abwn.block_time >= current_date - interval '17' day
    GROUP BY DATE(abwn.block_time)
),

liquidity_fee_tbl AS (
    SELECT
        DATE(a.block_time) AS block_date,
        COALESCE(SUM(a.liq_fee_in_rune_e8), 0) AS liquidity_fee
    FROM {{ ref('thorchain_silver_swap_events') }} a
    WHERE a.block_time >= current_date - interval '17' day
    GROUP BY DATE(a.block_time)
),

rewards_summary AS (
    SELECT
        date(tbr.block_time) AS block_date,
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
    WHERE tbr.block_time >= current_date - interval '17' day
    GROUP BY date(tbr.block_time)
),

base AS (
    SELECT
        abwnd.block_date,
        date_trunc('month', abwnd.block_date) as block_month,
        COALESCE((lft.liquidity_fee / power(10, 8)), 0) AS liquidity_fee,
        
        (COALESCE(rs.total_pool_rewards, 0) + COALESCE(rs.bond_earnings, 0)) AS block_rewards,
        
        (COALESCE(rs.total_pool_rewards, 0) + COALESCE(lft.liquidity_fee / power(10, 8), 0) + COALESCE(rs.bond_earnings, 0)) AS earnings,
        
        COALESCE(rs.bond_earnings, 0) AS bonding_earnings,
        
        (COALESCE(rs.total_pool_rewards, 0) + COALESCE(lft.liquidity_fee / power(10, 8), 0)) AS liquidity_earnings,
        
        abwnd.avg_nodes + 2 AS avg_node_count,
        abwnd._inserted_timestamp
        
    FROM all_block_with_nodes_date abwnd
    LEFT JOIN liquidity_fee_tbl lft
        ON abwnd.block_date = lft.block_date
    LEFT JOIN rewards_summary rs
        ON abwnd.block_date = rs.block_date
)

SELECT * FROM base