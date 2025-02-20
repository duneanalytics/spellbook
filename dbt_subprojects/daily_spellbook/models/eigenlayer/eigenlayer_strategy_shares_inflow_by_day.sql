{{ 
    config(
        schema = 'eigenlayer',
        alias = 'strategy_shares_inflow_by_day',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "eigenlayer",
                                    \'["bowenli"]\') }}'
    )
}}


WITH daily_share AS (
    SELECT
        strategy,
        SUM(share) as share,
        date_trunc('day', evt_block_time) AS date
    FROM {{ source('eigenlayer_ethereum', 'StrategyManager_evt_Deposit') }}
    GROUP BY strategy, date
)
SELECT
    a.date,
    b.strategy,
    COALESCE(b.share, 0) as share
FROM {{ ref('eigenlayer_date_series') }} AS a
LEFT JOIN daily_share AS b
    ON a.date = b.date
ORDER BY a.date DESC
