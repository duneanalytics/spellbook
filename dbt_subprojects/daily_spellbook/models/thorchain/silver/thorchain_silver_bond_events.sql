{{ config(
    schema = 'thorchain_silver',
    alias = 'bond_events',
    tags = ['thorchain', 'bond_events', 'silver']
) }}

with base as (
    SELECT
        tx AS tx_id,
        chain AS blockchain,
        from_addr AS from_address,
        to_addr AS to_address,
        asset,
        asset_e8,
        memo,
        bond_type,
        e8,
        event_id,
        block_timestamp,
        _tx_type,        
        _ingested_at AS _inserted_timestamp,        
        ROW_NUMBER() OVER(
            PARTITION BY tx, from_addr, asset_e8, bond_type, e8, block_timestamp, COALESCE(to_addr, ''), COALESCE(chain, ''), COALESCE(asset, ''), COALESCE(memo, '')
            ORDER BY _ingested_at DESC
        ) as rn
    FROM {{ source('thorchain', 'bond_events') }}
)

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
    event_id,
    block_timestamp,
    _tx_type,
    _inserted_timestamp
FROM base
WHERE rn = 1