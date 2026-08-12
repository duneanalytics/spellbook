{{ config(
    schema = 'thorchain_silver',
    alias = 'block_rewards',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['day'],
    partition_by = ['day'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')],
    tags = ['thorchain', 'block_rewards', 'silver']
) }}

-- ci-stamp: 1

WITH all_block_id AS (
    SELECT
        block_timestamp,
        MAX(_inserted_timestamp) AS _inserted_timestamp
    FROM {{ ref('thorchain_silver_block_pool_depths') }}
    {% if is_incremental() -%}
    WHERE {{ incremental_predicate('cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp)') }}
    {% endif -%}
    GROUP BY block_timestamp
)
, avg_nodes_tbl AS (
    SELECT
        block_timestamp,
        SUM(
            CASE
                WHEN current_status = 'Active' THEN 1
                WHEN former_status = 'Active' THEN -1
                ELSE 0
            END
        ) AS delta
    FROM {{ ref('thorchain_silver_update_node_account_status_events') }}
    {% if is_incremental() -%}
    WHERE {{ incremental_predicate('cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp)') }}
    {% endif -%}
    GROUP BY block_timestamp
)
, historical_node_count AS (
    SELECT
        {% if is_incremental() -%}
        -- Full history includes the two initial nodes added explicitly by the legacy full-build path.
        COALESCE(
            SUM(
                CASE
                    WHEN current_status = 'Active' THEN 1
                    WHEN former_status = 'Active' THEN -1
                    ELSE 0
                END
            ),
            0
        ) AS active_nodes
        FROM {{ ref('thorchain_silver_update_node_account_status_events') }}
        WHERE NOT {{ incremental_predicate('cast(from_unixtime(cast(block_timestamp / 1e9 as bigint)) as timestamp)') }}
        {% else -%}
        0 AS active_nodes
        {% endif -%}
)
, all_block_with_nodes AS (
    SELECT
        abi.block_timestamp,
        ant.delta,
        {% if is_incremental() -%}
        hnc.active_nodes + COALESCE(SUM(ant.delta) OVER (
            ORDER BY abi.block_timestamp ASC
        ), 0) AS avg_nodes,
        {% else -%}
        SUM(ant.delta) OVER (
            ORDER BY abi.block_timestamp ASC
        ) AS avg_nodes,
        {% endif -%}
        abi._inserted_timestamp
    FROM all_block_id as abi
    LEFT JOIN avg_nodes_tbl as ant
        ON abi.block_timestamp = ant.block_timestamp
    CROSS JOIN historical_node_count as hnc
)
, all_block_with_nodes_date AS (
    SELECT
        cast(date_trunc('day', b.block_timestamp) AS date) AS day,
        AVG(abwn.avg_nodes) AS avg_nodes,
        MAX(abwn._inserted_timestamp) AS _inserted_timestamp
    FROM all_block_with_nodes as abwn
    JOIN {{ ref('thorchain_silver_block_log') }} as b
        ON abwn.block_timestamp = b.timestamp
    {% if is_incremental() -%}
    WHERE {{ incremental_predicate('b.block_timestamp') }}
    {% endif -%}
    GROUP BY cast(date_trunc('day', b.block_timestamp) AS date)
)
, liquidity_fee_tbl AS (
    SELECT
        cast(date_trunc('day', b.block_timestamp) AS date) AS day,
        COALESCE(SUM(a.liq_fee_in_rune_e8), 0) AS liquidity_fee
    FROM {{ ref('thorchain_silver_swap_events') }} as a
    JOIN {{ ref('thorchain_silver_block_log') }} as b
        ON a.block_timestamp = b.timestamp
    {% if is_incremental() -%}
        AND {{ incremental_predicate('b.block_timestamp') }}
    WHERE {{ incremental_predicate('cast(from_unixtime(cast(a.block_timestamp / 1e9 as bigint)) as timestamp)') }}
    {% endif -%}
    GROUP BY cast(date_trunc('day', b.block_timestamp) AS date)
)
, bond_earnings_tbl AS (
    SELECT
        cast(date_trunc('day', b.block_timestamp) AS date) AS day,
        SUM(bond_e8) AS bond_earnings
    FROM
        {{ ref('thorchain_silver_rewards_events') }} as a
        JOIN {{ ref('thorchain_silver_block_log') }} as b
        ON a.block_timestamp = b.timestamp
        {% if is_incremental() -%}
            AND {{ incremental_predicate('b.block_timestamp') }}
        WHERE {{ incremental_predicate('cast(from_unixtime(cast(a.block_timestamp / 1e9 as bigint)) as timestamp)') }}
        {% endif -%}
    GROUP BY
        cast(date_trunc('day', b.block_timestamp) AS date)
)
, total_pool_rewards_tbl AS (
    SELECT
        cast(date_trunc('day', b.block_timestamp) AS date) AS day,
        SUM(rune_e8) AS total_pool_rewards
    FROM
        {{ ref('thorchain_silver_rewards_event_entries') }} as a
        JOIN {{ ref('thorchain_silver_block_log') }} as b
        ON a.block_timestamp = b.timestamp
        {% if is_incremental() -%}
            AND {{ incremental_predicate('b.block_timestamp') }}
        WHERE {{ incremental_predicate('cast(from_unixtime(cast(a.block_timestamp / 1e9 as bigint)) as timestamp)') }}
        {% endif -%}
    GROUP BY
        cast(date_trunc('day', b.block_timestamp) AS date)
)
SELECT
    all_block_with_nodes_date.day,
    COALESCE((liquidity_fee_tbl.liquidity_fee / power(10, 8)), 0) AS liquidity_fee,
    (
        (
        COALESCE(
            total_pool_rewards_tbl.total_pool_rewards,
            0
        ) + COALESCE(
            bond_earnings_tbl.bond_earnings,
            0
        )
        )
    ) / power(
        10,
        8
    ) AS block_rewards,
    (
        (
        COALESCE(
            total_pool_rewards_tbl.total_pool_rewards,
            0
        ) + COALESCE(
            liquidity_fee_tbl.liquidity_fee,
            0
        ) + COALESCE(
            bond_earnings_tbl.bond_earnings,
            0
        )
        )
    ) / power(
        10,
        8
    ) AS earnings,
    COALESCE((bond_earnings_tbl.bond_earnings / power(10, 8)), 0) AS bonding_earnings,
    (
        (
        COALESCE(
            total_pool_rewards_tbl.total_pool_rewards,
            0
        ) + COALESCE(
            liquidity_fee_tbl.liquidity_fee,
            0
        )
        )
    ) / power(
        10,
        8
    ) AS liquidity_earnings,
    {% if is_incremental() -%}
    all_block_with_nodes_date.avg_nodes AS avg_node_count,
    {% else -%}
    all_block_with_nodes_date.avg_nodes + 2 AS avg_node_count,
    {% endif -%}
    all_block_with_nodes_date._inserted_timestamp
FROM
    all_block_with_nodes_date
LEFT JOIN liquidity_fee_tbl
    ON all_block_with_nodes_date.day = liquidity_fee_tbl.day
LEFT JOIN total_pool_rewards_tbl
    ON all_block_with_nodes_date.day = total_pool_rewards_tbl.day
LEFT JOIN bond_earnings_tbl
    ON all_block_with_nodes_date.day = bond_earnings_tbl.day
