{{ config(
    schema = 'thorchain_defi',
    alias = 'fact_pool_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['fact_pool_events_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'defi', 'pool_events', 'fact', 'pools']
) }}

WITH base AS (
    SELECT
        asset,
        status,
        event_id,
        block_timestamp,
        block_time,
        block_month,
        _inserted_timestamp
    FROM {{ ref('thorchain_silver_pool_events') }}
    {% if is_incremental() %}
        WHERE {{ incremental_predicate('block_time') }}
    {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'a.event_id',
        'a.asset',
        'a.status',
        'a.block_timestamp'
    ]) }} AS fact_pool_events_id,
    b.block_time,
    b.block_timestamp,  -- Include for compatibility
    b.block_month,
    COALESCE(b.dim_block_id, '-1') AS dim_block_id,
    a.asset,
    a.status,
    a.event_id,
    a._inserted_timestamp,
    cast('{{ invocation_id }}' as varchar) AS _audit_run_id,
    current_timestamp AS inserted_timestamp,
    current_timestamp AS modified_timestamp
FROM base a
JOIN {{ ref('thorchain_core_dim_block') }} b
    ON a.block_timestamp = b.timestamp

