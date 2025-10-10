{{ config(
    schema = 'thorchain_defi',
    alias = 'fact_block_pool_depths',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['fact_pool_depths_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'defi', 'pool_depths', 'fact']
) }}

WITH base AS (
    SELECT
        pool_name,
        asset_e8,
        rune_e8,
        synth_e8,
        block_time,
        raw_block_timestamp,
        _inserted_timestamp
    FROM {{ ref('thorchain_silver_block_pool_depths') }}
    WHERE block_time >= current_date - interval '7' day
)

SELECT
    -- CRITICAL: Generate surrogate key (Trino equivalent of dbt_utils.generate_surrogate_key)
    to_hex(sha256(to_utf8(concat(
        COALESCE(a.pool_name, ''),
        '|',
        COALESCE(cast(a.block_time as varchar), '')
    )))) AS fact_pool_depths_id,
    
    -- CRITICAL: Always include partitioning columns first
    a.block_time,
    date(a.block_time) as block_date,
    date_trunc('month', a.block_time) as block_month,
    a.raw_block_timestamp,
    
    -- Block dimension reference (set directly - no JOIN needed)
    '-1' AS dim_block_id,
    
    -- Pool depth data
    a.rune_e8,
    a.asset_e8,
    a.synth_e8,
    a.pool_name,
    
    -- Audit fields (Trino conversions)
    a._inserted_timestamp,
    cast(from_hex(replace(cast(uuid() as varchar), '-', '')) as varchar) AS _audit_run_id,  -- Trino equivalent of invocation_id
    current_timestamp AS inserted_timestamp,  -- Trino equivalent of SYSDATE()
    current_timestamp AS modified_timestamp

FROM base a

{% if is_incremental() %}
WHERE {{ incremental_predicate('a.block_time') }}
{% endif %}
