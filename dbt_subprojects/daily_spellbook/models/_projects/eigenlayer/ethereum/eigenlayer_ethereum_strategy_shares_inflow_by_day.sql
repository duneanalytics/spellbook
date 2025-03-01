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


SELECT
    strategy,
    SUM(CAST(shares AS DECIMAL(38, 0))) as shares,
    date_trunc('day', evt_block_time) AS date
FROM {{ source('eigenlayer_ethereum', 'StrategyManager_evt_Deposit') }}
GROUP BY strategy, date_trunc('day', evt_block_time)

