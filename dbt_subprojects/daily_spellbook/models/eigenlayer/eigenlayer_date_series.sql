{{ 
    config(
        schema = 'eigenlayer',
        alias = 'date_series',
    )
}}


SELECT
    CAST(date_value AS DATE) AS date
FROM UNNEST(
    sequence(DATE '2024-02-01', CURRENT_DATE, INTERVAL '1' DAY)
) AS t(date_value)
ORDER BY date DESC
