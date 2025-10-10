{{ config(
    schema = 'thorchain_defi',
    alias = 'fact_daily_tvl',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['fact_daily_tvl_id'],
    partition_by = ['day_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')],
    tags = ['thorchain', 'defi', 'daily_tvl', 'fact']
) }}

WITH base AS (
    SELECT
        day,
        total_value_pooled,
        total_value_pooled_usd,
        total_value_bonded,
        total_value_bonded_usd,
        total_value_locked,
        total_value_locked_usd,
        day_month
    FROM {{ ref('thorchain_silver_daily_tvl') }}
    WHERE day >= current_date - interval '7' day
)

SELECT
    -- CRITICAL: Generate surrogate key (Trino equivalent of dbt_utils.generate_surrogate_key)
    to_hex(sha256(to_utf8(cast(a.day as varchar)))) AS fact_daily_tvl_id,
    
    -- CRITICAL: Always include partitioning columns first
    a.day,
    a.day_month,
    
    -- TVL data
    a.total_value_pooled,
    a.total_value_pooled_usd,
    a.total_value_bonded,
    a.total_value_bonded_usd,
    a.total_value_locked,
    a.total_value_locked_usd,
    
    -- Audit fields (Trino conversions)
    cast(from_hex(replace(cast(uuid() as varchar), '-', '')) as varchar) AS _audit_run_id,  -- Trino equivalent of invocation_id
    current_timestamp AS inserted_timestamp,  -- Trino equivalent of SYSDATE()
    current_timestamp AS modified_timestamp

FROM base a

{% if is_incremental() %}
WHERE {{ incremental_predicate('a.day') }}
{% endif %}
