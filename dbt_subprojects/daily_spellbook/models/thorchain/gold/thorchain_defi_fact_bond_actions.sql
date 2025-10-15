{{ config(
    schema = 'thorchain_defi',
    alias = 'fact_bond_actions',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['fact_bond_actions_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'defi', 'bond_actions', 'fact']
) }}

WITH block_prices AS (
    SELECT
        AVG(price) AS rune_usd,
        block_id
    FROM {{ ref('thorchain_silver_prices') }}
    WHERE symbol = 'RUNE'
    GROUP BY block_id
),

bond_events AS (
    SELECT
        block_timestamp,
        block_time,
        block_month,
        tx_id,
        from_address,
        to_address,
        asset,
        blockchain,
        bond_type,
        asset_e8,
        e8,
        memo,
        event_id,
        _tx_type,
        _inserted_timestamp
    FROM {{ ref('thorchain_silver_bond_events') }}
    {% if is_incremental() %}
        WHERE {{ incremental_predicate('block_time') }}
    {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'be.tx_id',
        'be.from_address',
        'be.to_address',
        'be.asset_e8',
        'be.bond_type',
        'be.e8',
        'be.block_timestamp',
        'be.blockchain',
        'be.asset',
        'be.memo'
    ]) }} AS fact_bond_actions_id,
    b.block_time,
    b.block_timestamp,  -- Include for compatibility
    b.block_month,
    COALESCE(b.dim_block_id, '-1') AS dim_block_id,
    be.tx_id,
    be.from_address,
    be.to_address,
    be.asset,
    be.blockchain,
    be.bond_type,
    COALESCE(be.e8 / power(10, 8), 0) AS asset_amount,
    COALESCE(p.rune_usd * be.asset_e8, 0) AS asset_usd,
    be.memo,
    be.event_id,
    be._tx_type,
    be._inserted_timestamp,
    cast('{{ invocation_id }}' as varchar) AS _audit_run_id,
    current_timestamp AS inserted_timestamp,
    current_timestamp AS modified_timestamp
FROM bond_events be
JOIN {{ ref('thorchain_core_dim_block') }} b
    ON be.block_timestamp = b.timestamp
LEFT JOIN block_prices p
    ON b.block_id = p.block_id

