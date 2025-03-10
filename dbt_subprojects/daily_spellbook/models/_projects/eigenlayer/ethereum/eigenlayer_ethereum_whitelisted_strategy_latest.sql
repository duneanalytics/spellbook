{{
    config(
        schema = 'eigenlayer_ethereum',
        alias = 'whitelisted_strategy_latest',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "eigenlayer",
                                    \'["bowenli"]\') }}',
        materialized = 'table',
        unique_key = ['strategy']
    )
}}


SELECT
    strategy
FROM {{ source('eigenlayer_ethereum', 'StrategyManager_evt_StrategyAddedToDepositWhitelist') }}
WHERE strategy NOT IN (
    SELECT strategy
    FROM {{ source('eigenlayer_ethereum', 'StrategyManager_evt_StrategyRemovedFromDepositWhitelist') }}
)