{{ 
    config(
        schema = 'eigenlayer_ethereum',
        alias = 'operator_shares_change_by_day',
        unique_key = ['operator', 'strategy', 'date']
    )
}}



WITH items AS (
    SELECT
        operator,
        strategy,
        CAST(shares AS DECIMAL(38,0)) AS shares,
        date_trunc('day', evt_block_time) AS date
    FROM {{ source('eigenlayer_ethereum', 'DelegationManager_evt_OperatorSharesIncreased') }}

    UNION ALL

    SELECT
        operator,
        strategy,
        CAST(shares AS DECIMAL(38,0)) * -1 AS shares,
        date_trunc('day', evt_block_time) AS date
    FROM {{ source('eigenlayer_ethereum', 'DelegationManager_evt_OperatorSharesDecreased') }}
)
SELECT
    operator,
    strategy,
    SUM(shares) as shares,
    date
FROM
    items
GROUP BY operator, strategy, date
ORDER BY date DESC