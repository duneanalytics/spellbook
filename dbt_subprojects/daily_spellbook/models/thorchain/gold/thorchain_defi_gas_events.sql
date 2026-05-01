{{ config(
    schema = 'thorchain',
    alias = 'defi_gas_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_month', 'fact_gas_events_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'defi', 'gas_events', 'fact'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                                  "project",
                                  "thorchain",
                                  \'["jeff-dude"]\') }}'
) }}

WITH base AS (
    SELECT
        asset,
        asset_e8,
        rune_e8,
        tx_count,
        event_id,
        block_timestamp,
        block_time,
        block_month,
        _inserted_timestamp
    FROM {{ ref('thorchain_silver_gas_events') }}
    {% if is_incremental() %}
        WHERE {{ incremental_predicate('block_time') }}
    {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'a.event_id',
        'a.asset',
        'a.block_timestamp'
    ]) }} AS fact_gas_events_id,
    b.block_time,
    b.block_timestamp,  -- Include for compatibility
    b.block_month,
    COALESCE(b.dim_block_id, '-1') AS dim_block_id,
    a.asset,
    a.asset_e8,
    a.rune_e8,
    a.tx_count,
    a._inserted_timestamp,
    cast('{{ invocation_id }}' as varchar) AS _audit_run_id,
    current_timestamp AS inserted_timestamp,
    current_timestamp AS modified_timestamp
FROM base a
JOIN {{ ref('thorchain_core_block') }} b
    ON a.block_timestamp = b.timestamp

