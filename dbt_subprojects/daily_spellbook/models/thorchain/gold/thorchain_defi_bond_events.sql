{{ config(
    schema = 'thorchain',
    alias = 'defi_bond_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['fact_bond_events_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'defi', 'bond_events', 'fact'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                              "defi",
                              "defi_bond_events",
                              \'["krishhh"]\') }}'
) }}

WITH base AS (
    SELECT
        tx_id,
        blockchain,
        from_address,
        to_address,
        asset,
        asset_e8,
        memo,
        bond_type,
        e8,
        block_timestamp,
        block_time,
        block_month,
        _tx_type,
        _inserted_timestamp
    FROM {{ ref('thorchain_silver_bond_events') }}
    {% if is_incremental() %}
        WHERE {{ incremental_predicate('block_time') }}
    {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'a.tx_id',
        'a.from_address',
        'a.to_address',
        'a.asset_e8',
        'a.bond_type',
        'a.e8',
        'a.block_timestamp',
        'a.blockchain',
        'a.asset',
        'a.memo'
    ]) }} AS fact_bond_events_id,
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
    a.memo,
    a.bond_type,
    a.e8,
    a._tx_type,
    a._inserted_timestamp,
    cast('{{ invocation_id }}' as varchar) AS _audit_run_id,
    current_timestamp AS inserted_timestamp,
    current_timestamp AS modified_timestamp
FROM base a
JOIN {{ ref('thorchain_core_block') }} b
    ON a.block_timestamp = b.timestamp

