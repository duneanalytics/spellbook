{{ 
    config(
        schema = 'eigenlayer_ethereum',
        alias = 'strategy_shares_outflow_by_day',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "eigenlayer",
                                    \'["bowenli"]\') }}',
        materialized = 'table',
        unique_key = ['strategy', 'date']
    )
}}


WITH combined_withdrawals AS (
    -- V1 withdrawal completed does not have shares data, nor can it be linked to withdrawal queued
    -- thus use withdrawal queued as replacement
    SELECT
        strategy,
        SUM(CAST(shares AS DECIMAL(38,0))) AS shares,
        date_trunc('day', evt_block_time) AS date
    FROM {{ source('eigenlayer_ethereum', 'StrategyManager_evt_ShareWithdrawalQueued') }}
    GROUP BY strategy, date_trunc('day', evt_block_time)

    UNION ALL

    SELECT
        strategy,
        SUM(shares) AS shares,
        date_trunc('day', evt_block_time) AS date
    FROM {{ ref('eigenlayer_ethereum_withdrawal_completed_v2_enriched') }}
    GROUP BY strategy, date_trunc('day', evt_block_time)

    UNION ALL

    -- native ETH strategy
    SELECT
        strategy,
        SUM(shares) AS shares,
        date_trunc('day', evt_block_time) AS date
    FROM {{ ref('eigenlayer_ethereum_pod_shares_updated_enriched') }}
    WHERE shares < 0
    GROUP BY strategy, date_trunc('day', evt_block_time)
)
SELECT
    strategy,
    SUM(shares) * -1 as shares,
    date
FROM combined_withdrawals
GROUP BY strategy, date
