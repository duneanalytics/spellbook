{{ config(
    schema = 'thorchain_silver',
    alias = 'swap_events',
    tags = ['thorchain', 'swaps', 'dex']
) }}

with base as (
    SELECT
        tx as tx_id,
        chain as blockchain,
        from_addr as from_address,
        to_addr as to_address,
        from_asset,
        from_e8,
        to_asset,
        to_e8,
        memo,
        pool as pool_name,
        to_e8_min,
        swap_slip_bp,
        liq_fee_e8,
        liq_fee_in_rune_e8,
        _direction,
        event_id,
        block_timestamp,
        streaming_count,
        streaming_quantity,
        _tx_type,
        _ingested_at as _inserted_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY event_id, tx, chain, to_addr, from_addr, from_asset, from_e8, to_asset, to_e8, memo, pool, _direction
            ORDER BY _ingested_at DESC
        ) as rn
    FROM {{ source('thorchain', 'swap_events') }}
)

SELECT *
FROM base
WHERE rn = 1