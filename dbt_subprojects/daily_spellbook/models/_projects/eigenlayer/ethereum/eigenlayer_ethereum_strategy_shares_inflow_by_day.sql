{{ 
    config(
        schema = 'eigenlayer_ethereum',
        alias = 'strategy_shares_inflow_by_day',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "eigenlayer",
                                    \'["bowenli"]\') }}',
        materialized = 'table',
        unique_key = ['strategy', 'date']
    )
}}


WITH daily_share AS (
    SELECT
        strategy,
        SUM(CAST(shares AS DECIMAL(38, 0))) as shares,
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
    strategy,
    SUM(shares) as shares,
    date
FROM daily_share
GROUP BY strategy, date
