{{ 
    config(
        schema = 'eigenlayer_ethereum',
        alias = 'operator_shares_cumulative_by_day',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "eigenlayer",
                                    \'["bowenli"]\') }}',
        unique_key = ['operator', 'strategy', 'date']
    )
}}


WITH eigenlayer_ethereum_date_series AS (
    SELECT
        timestamp as date
    FROM
        {{ source('utils', 'days') }}
    WHERE
        timestamp >= date '2024-02-01'
),
all_operator_strategy AS (
    SELECT
        DISTINCT operator, strategy
    FROM
        {{ ref('eigenlayer_ethereum_operator_shares_change_by_day') }}
),
daily_aggregated_shares AS (
    SELECT
        a.operator,
        a.strategy,
        b.date,
        COALESCE(SUM(c.shares), 0) AS shares
    FROM
        all_operator_strategy AS a
    CROSS JOIN
        eigenlayer_ethereum_date_series AS b
    LEFT JOIN
        {{ ref('eigenlayer_ethereum_operator_shares_change_by_day') }} AS c
    ON a.operator = c.operator
        AND a.strategy = c.strategy
        AND b.date = c.date
    GROUP BY
        a.operator,
        a.strategy,
        b.date
),
cumulative_shares AS (
    SELECT
        operator,
        strategy,
        date,
        SUM(shares) OVER (PARTITION BY operator, strategy ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_daily_shares
    FROM
        daily_aggregated_shares
)
SELECT
    operator,
    strategy,
    date,
    cumulative_daily_shares
FROM
    cumulative_shares
ORDER BY
    operator,
    strategy,
    date DESC
