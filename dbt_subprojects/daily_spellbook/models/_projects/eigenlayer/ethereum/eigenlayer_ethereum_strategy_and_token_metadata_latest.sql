{{
    config(
        schema = 'eigenlayer_ethereum',
        alias = 'strategy_and_token_metadata_latest',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "eigenlayer",
                                    \'["bowenli"]\') }}',
        materialized = 'table',
        unique_key = ['strategy']
    )
}}



SELECT
    a.strategy,
    a.token,
    b.symbol,
    b.decimals
FROM {{ source('eigenlayer_ethereum', 'StrategyFactory_evt_StrategySetForToken') }} AS a
JOIN {{ source('tokens', 'erc20') }} AS b
ON a.token = b.contract_address
WHERE b.blockchain = 'ethereum'
AND strategy IN (
    SELECT
        strategy
    FROM {{ ref('eigenlayer_ethereum_whitelisted_strategy_latest') }}
)


UNION ALL


SELECT
    strategy,
    token,
    name AS symbol,
    18 AS decimals
FROM {{ ref('eigenlayer_ethereum_strategy_category') }}
