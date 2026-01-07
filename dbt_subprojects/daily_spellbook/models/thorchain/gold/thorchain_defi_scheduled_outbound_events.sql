{{ config(
    schema = 'thorchain',
    alias = 'defi_scheduled_outbound_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_month', 'fact_scheduled_outbound_events_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'defi', 'scheduled_outbound_events', 'fact'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                                  "project",
                                  "thorchain",
                                  \'["jeff-dude"]\') }}'
) }}

WITH base AS (
    SELECT
        blockchain,
        to_address,
        asset,
        asset_e8,
        asset_decimals,
        gas_rate,
        memo,
        in_hash,
        out_hash,
        max_gas_amount,
        max_gas_decimals,
        max_gas_asset,
        module_name,
        vault_pub_key,
        event_id,
        block_timestamp,
        block_time,
        block_month,
        _inserted_timestamp
    FROM {{ ref('thorchain_silver_scheduled_outbound_events') }}
    {% if is_incremental() %}
        WHERE {{ incremental_predicate('block_time') }}
    {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'a.blockchain',
        'a.to_address',
        'a.asset',
        'a.asset_e8',
        'a.asset_decimals',
        'a.gas_rate',
        'a.memo',
        'a.in_hash',
        'a.out_hash',
        'a.max_gas_amount',
        'a.max_gas_decimals',
        'a.max_gas_asset',
        'a.module_name',
        'a.vault_pub_key',
        'a.event_id',
        'a.block_timestamp'
    ]) }} AS fact_scheduled_outbound_events_id,
    b.block_time,
    b.block_timestamp,  -- Include for compatibility
    b.block_month,
    COALESCE(b.dim_block_id, '-1') AS dim_block_id,
    a.blockchain,
    a.to_address,
    a.asset,
    a.asset_e8,
    a.asset_decimals,
    a.gas_rate,
    a.memo,
    a.in_hash,
    a.out_hash,
    a.max_gas_amount,
    a.max_gas_decimals,
    a.max_gas_asset,
    a.module_name,
    a.vault_pub_key,
    a.event_id,
    a._inserted_timestamp,
    cast('{{ invocation_id }}' as varchar) AS _audit_run_id,
    current_timestamp AS inserted_timestamp,
    current_timestamp AS modified_timestamp
FROM base a
JOIN {{ ref('thorchain_core_block') }} b
    ON a.block_timestamp = b.timestamp

