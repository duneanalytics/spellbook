{{ config(
    schema = 'thorchain_silver',
    alias = 'block_log',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_month', 'height'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'block_log', 'silver', 'dimension']
) }}

WITH deduplicated AS (
    SELECT
        height,
        timestamp,
        hash,
        agg_state,
        _updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY height
            ORDER BY _updated_at DESC
        ) AS rn
    FROM {{ source('thorchain', 'block_log') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('cast(from_unixtime(cast(timestamp / 1e9 as bigint)) as timestamp)') }}
    {% endif %}
),

base AS (
    SELECT
        height,
        cast(from_unixtime(cast(timestamp / 1e9 as bigint)) as timestamp) AS block_time,
        cast(from_unixtime(cast(timestamp / 1e9 as bigint)) as timestamp) AS block_timestamp,
        timestamp,
        hash,
        agg_state,
        current_timestamp AS _inserted_timestamp
    FROM deduplicated
    WHERE rn = 1
)

SELECT
    height,
    block_timestamp,
    block_time,
    date(block_time) AS block_date,
    date_trunc('hour', block_time) AS block_hour,
    date_trunc('week', block_time) AS block_week,
    date_trunc('month', block_time) AS block_month,
    date_trunc('quarter', block_time) AS block_quarter,
    date_trunc('year', block_time) AS block_year,
    day(block_time) AS block_dayofmonth,
    day_of_week(block_time) AS block_dayofweek,
    day_of_year(block_time) AS block_dayofyear,
    timestamp,
    hash,
    agg_state,
    _inserted_timestamp
FROM base

