{{ config(
    schema = 'thorchain',
    alias = 'defi_bond_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['fact_bond_events_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_timestamp')],
    tags = ['thorchain', 'defi', 'bond_events', 'fact'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                                  "project",
                                  "thorchain",
                                  \'["jeff-dude"]\') }}'
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
        _tx_type,
        _inserted_timestamp
    FROM
        {{ ref('thorchain_silver_bond_events') }}
)
SELECT
    {{ dbt_utils.generate_surrogate_key(
        ['a.tx_id','a.from_address','a.to_address ','a.asset_e8','a.bond_type','a.e8','a.block_timestamp','a.blockchain','a.asset','a.memo']
    ) }} AS fact_bond_events_id,
    b.block_timestamp,
    COALESCE(
        b.dim_block_id,
        '-1'
    ) AS dim_block_id,
    tx_id,
    blockchain,
    from_address,
    to_address,
    asset,
    asset_e8,
    memo,
    bond_type,
    e8,
    _TX_TYPE,
    A._INSERTED_TIMESTAMP,
    current_timestamp AS inserted_timestamp,
    current_timestamp AS modified_timestamp
FROM
    base as a
JOIN {{ ref('thorchain_core_block') }} as b
    ON a.block_timestamp = b.timestamp
{% if is_incremental() %}
WHERE {{ incremental_predicate('b.block_timestamp') }}
OR tx_id IN (
    SELECT
        tx_id
    FROM
        {{ this }}
    WHERE
        dim_block_id = '-1'
)
{% endif -%}
