{{ 
    config(
        schema = 'eigenlayer_ethereum',
        alias = 'strategy_shares_outflow_by_day',
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
combined_withdrawals AS (
    -- V1 withdrawal completed does not have shares data, nor can it be linked to withdrawal queued
    -- thus use withdrawal queued as replacement
    SELECT
        strategy,
        CAST(shares AS DECIMAL(38,0)) AS shares,
        date_trunc('day', evt_block_time) AS date
    FROM {{ source('eigenlayer_ethereum', 'StrategyManager_evt_ShareWithdrawalQueued') }}

    UNION ALL

    SELECT
        strategy,
        shares,
        date_trunc('day', evt_block_time) AS date
    FROM {{ ref('eigenlayer_ethereum_withdrawal_completed_v2_enriched') }}
),
daily_share AS (
    SELECT
        strategy,
        SUM(shares) * -1 as shares,
        date
    FROM combined_withdrawals
    GROUP BY strategy, date
)
SELECT
    a.date,
    b.strategy,
    COALESCE(b.shares, 0) as shares
FROM eigenlayer_ethereum_date_series AS a
LEFT JOIN daily_share AS b
    ON a.date = b.date
ORDER BY a.date DESC
