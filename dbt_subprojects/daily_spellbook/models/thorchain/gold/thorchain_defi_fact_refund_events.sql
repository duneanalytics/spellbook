{{ config(
    schema = 'thorchain_defi',
    alias = 'fact_refund_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['fact_refund_events_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'defi', 'refund_events', 'fact']
) }}

WITH base AS (
    SELECT
        tx_id,
        blockchain,
        from_address,
        to_address,
        asset,
        asset_e8,
        asset_2nd,
        asset_2nd_e8,
        memo,
        code,
        reason,
        event_id,
        block_timestamp,
        block_time,
        block_month,
        _tx_type,
        _inserted_timestamp
    FROM {{ ref('thorchain_silver_refund_events') }}
    WHERE block_time >= current_date - interval '16' day
    {% if is_incremental() %}
      AND {{ incremental_predicate('block_time') }}
    {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'a.event_id',
        'a.tx_id',
        'a.blockchain',
        'a.from_address',
        'a.to_address',
        'a.asset',
        'a.asset_2nd',
        'a.memo',
        'a.code',
        'a.reason',
        'a.block_timestamp'
    ]) }} AS fact_refund_events_id,
    b.block_time,
    b.block_timestamp,  -- Include for compatibility
    b.block_month,
    COALESCE(b.dim_block_id, '-1') AS dim_block_id,
    a.tx_id,
    a.blockchain,
    a.from_address,
    a.to_address,
    a.asset,
    a.asset_e8,
    a.asset_2nd,
    a.asset_2nd_e8,
    a.memo,
    a.code,
    a.reason,
    a._tx_type,
    a._inserted_timestamp,
    cast('{{ invocation_id }}' as varchar) AS _audit_run_id,
    current_timestamp AS inserted_timestamp,
    current_timestamp AS modified_timestamp
FROM base a
JOIN {{ ref('thorchain_core_dim_block') }} b
    ON a.block_timestamp = b.timestamp

