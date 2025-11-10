{{ config(
    schema = 'thorchain_silver',
    alias = 'withdraw_events',
    materialized = 'view',
    tags = ['thorchain', 'liquidity', 'withdraw_events']  
) }}

with base as (
    SELECT
        tx AS tx_id,
        chain AS blockchain,
        from_addr AS from_address,
        to_addr AS to_address,
        asset,
        asset_e8,
        emit_asset_e8,
        emit_rune_e8,
        memo,
        pool AS pool_name,
        stake_units,
        basis_points,
        asymmetry,
        imp_loss_protection_e8,
        _emit_asset_in_rune_e8,
        event_id,
        block_timestamp,
        _TX_TYPE,
        _ingested_at AS _inserted_timestamp,
    FROM {{ source('thorchain', 'withdraw_events') }}
)

SELECT * FROM base
