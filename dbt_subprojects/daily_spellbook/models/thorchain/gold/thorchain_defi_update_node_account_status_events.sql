{{ config(
    schema = 'thorchain',
    alias = 'defi_update_node_account_status_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['fact_update_node_account_status_events_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'defi', 'update_node_account_status_events', 'fact'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                              "defi",
                              "defi_update_node_account_status_events",
                              \'["krishhh"]\') }}'
) }}

WITH base AS (
    SELECT
        node_address,
        current_status,
        former_status,
        block_timestamp,
        event_id,
        block_time,
        block_month,
        _inserted_timestamp
    FROM {{ ref('thorchain_silver_update_node_account_status_events') }}
    {% if is_incremental() %}
        WHERE {{ incremental_predicate('block_time') }}
    {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'a.node_address',
        'a.block_timestamp',
        'a.current_status',
        'a.former_status'
    ]) }} AS fact_update_node_account_status_events_id,
    b.block_time,
    b.block_timestamp,  -- Include for compatibility
    b.block_month,
    COALESCE(b.dim_block_id, '-1') AS dim_block_id,
    a.former_status,
    a.current_status,
    a.node_address,
    a.event_id,
    a._inserted_timestamp,
    cast('{{ invocation_id }}' as varchar) AS _audit_run_id,
    current_timestamp AS inserted_timestamp,
    current_timestamp AS modified_timestamp
FROM base a
JOIN {{ ref('thorchain_core_dim_block') }} b
    ON a.block_timestamp = b.timestamp

