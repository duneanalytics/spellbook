{{ 
    config(
        schema = 'eigenlayer',
        alias = 'operator_shares_change_by_day',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "eigenlayer",
                                    \'["bowenli"]\') }}'
    )
}}



WITH items AS (
    SELECT
        operator,
        strategy,
        shares,
        date_trunc('day', evt_block_time) AS date
    FROM {{ source('eigenlayer_ethereum', 'DelegationManager_evt_OperatorSharesIncreased') }}

    UNION ALL

    SELECT
        operator,
        strategy,
        shares * -1 AS shares,
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