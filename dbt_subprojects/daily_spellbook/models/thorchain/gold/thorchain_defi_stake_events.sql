{{ config(
    schema = 'thorchain',
    alias = 'defi_stake_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['day', 'fact_stake_events_id'],
    partition_by = ['day'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_timestamp')],
    tags = ['thorchain', 'defi', 'stake_events', 'fact', 'staking'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                                  "project",
                                  "thorchain",
                                  \'["jeff-dude"]\') }}'
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
        _ASSET_IN_RUNE_E8,
        event_id,
        block_timestamp,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('thorchain_silver_stake_events') }}
)
SELECT
    {{ dbt_utils.generate_surrogate_key(
        ['a.event_id', 'a.pool_name', 'a.asset_blockchain', 'a.stake_units', 'a.rune_address', 'a.asset_tx_id', 'a.asset_address', 'a.block_timestamp']
    ) }} AS fact_stake_events_id,
    cast(date_trunc('day', b.block_timestamp) AS date) AS day,
    b.block_timestamp,
    COALESCE(
        b.dim_block_id,
        '-1'
    ) AS dim_block_id,
    pool_name,
    asset_tx_id,
    asset_blockchain,
    asset_address,
    asset_e8,
    stake_units,
    rune_tx_id,
    rune_address,
    rune_e8,
    _ASSET_IN_RUNE_E8,
    A._inserted_timestamp,
    current_timestamp AS inserted_timestamp,
    current_timestamp AS modified_timestamp
FROM
    base A
JOIN {{ ref('thorchain_core_block') }} as b
    ON A.block_timestamp = b.timestamp
{% if is_incremental() %}
WHERE {{ incremental_predicate('b.block_timestamp') }}
{% endif %}
