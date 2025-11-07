{{ config(
    schema = 'thorchain',
    alias = 'defi_stake_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_month', 'fact_stake_events_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'defi', 'stake_events', 'fact', 'staking'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                              "defi",
                              "defi_stake_events",
                              \'["krishhh"]\') }}'
) }}

WITH base AS (
    SELECT
        pool_name,
        asset_tx_id,
        asset_blockchain,
        asset_address,
        asset_e8,
        stake_units,
        rune_tx_id,
        rune_address,
        rune_e8,
        _asset_in_rune_e8,
        event_id,
        block_timestamp,
        block_time,
        block_month,
        _inserted_timestamp
    FROM {{ ref('thorchain_silver_stake_events') }}
    {% if is_incremental() %}
        WHERE {{ incremental_predicate('block_time') }}
    {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'a.event_id',
        'a.pool_name',
        'a.asset_blockchain',
        'a.stake_units',
        'a.rune_address',
        'a.asset_tx_id',
        'a.asset_address',
        'a.block_timestamp'
    ]) }} AS fact_stake_events_id,
    b.block_time,
    b.block_timestamp,  -- Include for compatibility
    b.block_month,
    COALESCE(b.dim_block_id, '-1') AS dim_block_id,
    a.pool_name,
    a.asset_tx_id,
    a.asset_blockchain,
    a.asset_address,
    a.asset_e8,
    a.stake_units,
    a.rune_tx_id,
    a.rune_address,
    a.rune_e8,
    a._asset_in_rune_e8,
    a.event_id,
    a._inserted_timestamp,
    cast('{{ invocation_id }}' as varchar) AS _audit_run_id,
    current_timestamp AS inserted_timestamp,
    current_timestamp AS modified_timestamp
FROM base a
JOIN {{ ref('thorchain_core_block') }} b
    ON a.block_timestamp = b.timestamp

