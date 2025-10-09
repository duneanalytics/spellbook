{{ config(
    schema = 'thorchain_core',
    alias = 'dim_block',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['height'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'core', 'blocks']
) }}

-- Core block dimension table for Thorchain
-- Provides clean joins and partitioning by block_date
SELECT
    cast(height as bigint) as height,
    hash as block_hash,
    cast(from_unixtime(cast(timestamp / 1e9 as bigint)) as timestamp) as block_time,
    date(from_unixtime(cast(timestamp / 1e9 as bigint))) as block_date,
    date_trunc('month', from_unixtime(cast(timestamp / 1e9 as bigint))) as block_month,
    date_trunc('hour', from_unixtime(cast(timestamp / 1e9 as bigint))) as block_hour,
    date_trunc('week', from_unixtime(cast(timestamp / 1e9 as bigint))) as block_week,
    year(from_unixtime(cast(timestamp / 1e9 as bigint))) as block_year,
    quarter(from_unixtime(cast(timestamp / 1e9 as bigint))) as block_quarter,
    timestamp as raw_timestamp,
    agg_state,
    
    -- Additional time-based dimensions for analytics
    day_of_week(from_unixtime(cast(timestamp / 1e9 as bigint))) as day_of_week,
    hour(from_unixtime(cast(timestamp / 1e9 as bigint))) as hour_of_day,
    
    -- Flag for weekend vs weekday (Trino: 1=Monday, 7=Sunday)
    CASE 
        WHEN day_of_week(from_unixtime(cast(timestamp / 1e9 as bigint))) IN (6, 7) THEN true
        ELSE false
    END as is_weekend,
    
    -- Business day classification (Monday-Friday, Trino: 1=Monday)
    CASE 
        WHEN day_of_week(from_unixtime(cast(timestamp / 1e9 as bigint))) BETWEEN 1 AND 5 THEN true
        ELSE false
    END as is_business_day,
    
    -- Time period classifications for analytics
    CASE 
        WHEN hour(from_unixtime(cast(timestamp / 1e9 as bigint))) BETWEEN 0 AND 5 THEN 'night'
        WHEN hour(from_unixtime(cast(timestamp / 1e9 as bigint))) BETWEEN 6 AND 11 THEN 'morning'
        WHEN hour(from_unixtime(cast(timestamp / 1e9 as bigint))) BETWEEN 12 AND 17 THEN 'afternoon'
        WHEN hour(from_unixtime(cast(timestamp / 1e9 as bigint))) BETWEEN 18 AND 23 THEN 'evening'
        ELSE 'unknown'
    END as time_period

FROM {{ source('thorchain', 'block_log') }}
WHERE height IS NOT NULL
{% if is_incremental() %}
AND {{ incremental_predicate('block_time') }}
  AND block_time >= current_date - interval '7' day
{% endif %}
