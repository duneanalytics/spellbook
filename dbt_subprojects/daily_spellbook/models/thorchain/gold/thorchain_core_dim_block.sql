{{ config(
    schema = 'thorchain_core',
    alias = 'dim_block',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['height'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'core', 'blocks']
) }}

-- Core block dimension table for Thorchain
-- Provides clean joins and partitioning by block_date
SELECT
    cast(height as bigint) as height,
    hash as block_hash,
    cast(from_unixtime(timestamp / 1e18) as timestamp) as block_time,
    date(from_unixtime(timestamp / 1e18)) as block_date,
    date_trunc('month', from_unixtime(timestamp / 1e18)) as block_month,
    date_trunc('hour', from_unixtime(timestamp / 1e18)) as block_hour,
    date_trunc('week', from_unixtime(timestamp / 1e18)) as block_week,
    extract(year from from_unixtime(timestamp / 1e18)) as block_year,
    extract(quarter from from_unixtime(timestamp / 1e18)) as block_quarter,
    timestamp as raw_timestamp,
    agg_state,
    
    -- Additional time-based dimensions for analytics
    extract(dayofweek from from_unixtime(timestamp / 1e18)) as day_of_week,
    extract(hour from from_unixtime(timestamp / 1e18)) as hour_of_day,
    
    -- Flag for weekend vs weekday
    CASE 
        WHEN extract(dayofweek from from_unixtime(timestamp / 1e18)) IN (1, 7) THEN true
        ELSE false
    END as is_weekend,
    
    -- Business day classification (Monday-Friday)
    CASE 
        WHEN extract(dayofweek from from_unixtime(timestamp / 1e18)) BETWEEN 2 AND 6 THEN true
        ELSE false
    END as is_business_day,
    
    -- Time period classifications for analytics
    CASE 
        WHEN extract(hour from from_unixtime(timestamp / 1e18)) BETWEEN 0 AND 5 THEN 'night'
        WHEN extract(hour from from_unixtime(timestamp / 1e18)) BETWEEN 6 AND 11 THEN 'morning'
        WHEN extract(hour from from_unixtime(timestamp / 1e18)) BETWEEN 12 AND 17 THEN 'afternoon'
        WHEN extract(hour from from_unixtime(timestamp / 1e18)) BETWEEN 18 AND 23 THEN 'evening'
        ELSE 'unknown'
    END as time_period

FROM {{ source('thorchain', 'block_log') }}
WHERE height IS NOT NULL
{% if is_incremental() %}
AND {{ incremental_predicate('cast(from_unixtime(timestamp / 1e18) as timestamp)') }}
{% endif %}
