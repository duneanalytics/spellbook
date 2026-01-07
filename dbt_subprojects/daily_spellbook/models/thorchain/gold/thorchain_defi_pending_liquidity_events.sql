{{ config(
    schema = 'thorchain',
    alias = 'defi_pending_liquidity_events',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_month', 'fact_pending_liquidity_events_id'],
    partition_by = ['block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    tags = ['thorchain', 'defi', 'pending_liquidity_events', 'fact', 'liquidity'],
    post_hook='{{ expose_spells(\'["thorchain"]\',
                                  "project",
                                  "thorchain",
                                  \'["jeff-dude"]\') }}'
) }}

WITH base AS (
    SELECT
        pool AS pool_name,
        asset_tx AS asset_tx_id,
        asset_chain AS asset_blockchain,
        asset_addr AS asset_address,
        asset_e8,
        asset_amount,
        rune_tx AS rune_tx_id,
        rune_addr AS rune_address,
        rune_e8,
        rune_amount,
        pending_type,
        event_id,
        raw_block_timestamp AS block_timestamp,
        block_time,
        block_month,
        pool_chain,
        pool_asset
    FROM {{ ref('thorchain_silver_pending_liquidity_events') }}
    {% if is_incremental() %}
        WHERE {{ incremental_predicate('block_time') }}
    {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'a.event_id',
        'a.pool_name',
        'a.asset_tx_id',
        'a.asset_blockchain',
        'a.asset_address',
        'a.rune_tx_id',
        'a.rune_address',
        'a.pending_type',
        'a.block_timestamp'
    ]) }} AS fact_pending_liquidity_events_id,
    b.block_time,
    b.block_timestamp,  -- Include for compatibility
    b.block_month,
    COALESCE(b.dim_block_id, '-1') AS dim_block_id,
    a.pool_name,
    a.asset_tx_id,
    a.asset_blockchain,
    a.asset_address,
    a.asset_e8,
    a.asset_amount,
    a.rune_tx_id,
    a.rune_address,
    a.rune_e8,
    a.rune_amount,
    a.pending_type,
    a.event_id,
    a.pool_chain,
    a.pool_asset,
    current_timestamp AS _inserted_timestamp,
    cast('{{ invocation_id }}' as varchar) AS _audit_run_id,
    current_timestamp AS inserted_timestamp,
    current_timestamp AS modified_timestamp
FROM base a
JOIN {{ ref('thorchain_core_block') }} b
    ON a.block_timestamp = b.timestamp

