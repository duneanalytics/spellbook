{{ 
    config(
        schema = 'eigenlayer',
        alias = 'operator_shares_cumulative_by_day',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "eigenlayer",
                                    \'["bowenli"]\') }}'
    )
}}


WITH all_operator_strategy AS (
    SELECT
        DISTINCT operator, strategy
    FROM
        {{ ref('eigenlayer_operator_shares_change_by_day') }}
),
daily_aggregated_shares AS (
    SELECT
        a.operator,
        a.strategy,
        b.date,
        COALESCE(SUM(c.daily_shares), 0) AS daily_shares
    FROM
        all_operator_strategy AS a
    CROSS JOIN
        {{ ref('eigenlayer_date_series') }} AS b
    LEFT JOIN
        {{ ref('eigenlayer_operator_shares_change_by_day') }} AS c
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
        SUM(daily_shares) OVER (PARTITION BY operator, strategy ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_daily_shares
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
