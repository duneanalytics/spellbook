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


SELECT
    strategy,
    SUM(share) as share,
    date_trunc('day', evt_block_time) AS date
FROM {{ source('eigenlayer_ethereum', 'StrategyManager_evt_Deposit') }}
GROUP BY strategy, date
ORDER BY date DESC
