{{ config(
    schema = 'thorchain',
    alias = 'core_transfer_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['fact_transfer_events_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'core', 'transfer_events', 'fact'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                              "core",
                              "core_transfer_events",
                              \'["krishhh"]\') }}'
) }}

WITH base AS (
    SELECT
        from_address,
        to_address,
        asset,
        amount_e8,
        event_id,
        block_timestamp,
        block_time,
        block_month,
        _inserted_timestamp
    FROM {{ ref('thorchain_silver_transfer_events') }}
    {% if not is_incremental() %}
        WHERE block_time >= current_date - interval '16' day
    {% endif %}
    {% if is_incremental() %}
        WHERE {{ incremental_predicate('block_time') }}
    {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'a.event_id',
        'a.from_address', 
        'a.to_address',
        'a.asset',
        'a.amount_e8'
    ]) }} AS fact_transfer_events_id,
    b.block_time,
    b.block_timestamp,  -- Include for compatibility
    b.block_month,
    COALESCE(b.dim_block_id, '-1') AS dim_block_id,
    a.from_address,
    a.to_address,
    a.asset,
    a.amount_e8,
    a._inserted_timestamp,
    cast('{{ invocation_id }}' as varchar) AS _audit_run_id,
    current_timestamp AS inserted_timestamp,
    current_timestamp AS modified_timestamp
FROM base a
JOIN {{ ref('thorchain_core_block') }} b
    ON a.block_timestamp = b.timestamp

