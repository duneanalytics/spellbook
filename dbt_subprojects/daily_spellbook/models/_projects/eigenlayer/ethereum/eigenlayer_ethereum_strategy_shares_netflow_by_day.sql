{{ 
    config(
        schema = 'eigenlayer_ethereum',
        alias = 'strategy_shares_netflow_by_day',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "eigenlayer",
                                    \'["bowenli"]\') }}',
        materialized = 'table',
        unique_key = ['strategy', 'date']
    )
}}


WITH combined AS (
    SELECT
        strategy,
        shares,
        date
    FROM {{ ref('eigenlayer_ethereum_strategy_shares_inflow_by_day') }}


    UNION ALL


    SELECT
        strategy,
        shares,
        date
    FROM {{ ref('eigenlayer_ethereum_strategy_shares_outflow_by_day') }}
)
SELECT
    strategy,
    SUM(shares) as shares,
    date
FROM combined
GROUP BY strategy, date
ORDER BY date DESC
