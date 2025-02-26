{{ 
    config(
        schema = 'eigenlayer_ethereum',
        alias = 'strategy_shares_inflow_by_day',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "eigenlayer",
                                    \'["bowenli"]\') }}'
    )
}}


WITH eigenlayer_ethereum_date_series AS (
    SELECT
        timestamp as date
    FROM
        {{ source('utils', 'days') }}
    WHERE
        timestamp >= date '2024-02-01'
),
daily_share AS (
    SELECT
        strategy,
        SUM(shares) as shares,
        date_trunc('day', evt_block_time) AS date
    FROM {{ source('eigenlayer_ethereum', 'StrategyManager_evt_Deposit') }}
    GROUP BY strategy, date_trunc('day', evt_block_time)

    UNION ALL

    -- native ETH strategy
    SELECT
        strategy,
        SUM(shares) as shares,
        date_trunc('day', evt_block_time) AS date
    FROM {{ ref('eigenlayer_ethereum_pod_shares_updated_enriched') }}
    WHERE shares > 0
    GROUP BY strategy, date_trunc('day', evt_block_time)
)
SELECT
    a.date,
    b.strategy,
    COALESCE(b.shares, 0) as shares
FROM eigenlayer_ethereum_date_series AS a
LEFT JOIN daily_share AS b
    ON a.date = b.date
ORDER BY a.date DESC
