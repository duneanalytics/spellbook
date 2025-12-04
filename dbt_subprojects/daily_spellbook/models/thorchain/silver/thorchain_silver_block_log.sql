{{ config(
    schema = 'thorchain_silver',
    alias = 'block_log',
    tags = ['thorchain', 'block_log', 'silver', 'dimension']
) }}

with base as (
    SELECT
        height
        , block_time as block_timestamp
        , timestamp
        , hash
        , agg_state
        , _ingested_at as _inserted_timestamp
        , ROW_NUMBER() OVER (
            PARTITION BY height
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM {{ source('thorchain', 'block_log') }}
)

SELECT
    height,
    block_timestamp,
    cast(date_trunc('day', block_timestamp) AS date) AS block_date,
    date_trunc('hour', block_timestamp) AS block_hour,
    date_trunc('week', block_timestamp) AS block_week,
    date_trunc('month', block_timestamp) AS block_month,
    date_trunc('quarter', block_timestamp) AS block_quarter,
    date_trunc('year', block_timestamp) AS block_year,
    day(block_timestamp) AS block_dayofmonth,
    day_of_week(block_timestamp) AS block_dayofweek,
    day_of_year(block_timestamp) AS block_dayofyear,
    timestamp,
    hash,
    agg_state,
    _inserted_timestamp
FROM base
WHERE rn = 1