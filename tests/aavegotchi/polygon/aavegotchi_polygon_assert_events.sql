-- Check if all aavegotchi events make it into the processed events table
WITH raw_events AS (
    SELECT evt_block_time as raw_block_time,
        evt_tx_hash as raw_tx_hash,
        evt_tx_hash || '-Trade-' || evt_index || '-' || erc721TokenId AS raw_unique_trade_id
    from {{ source ('aavegotchi_polygon', 'aavegotchi_diamond_evt_ERC721ExecutedListing') }}
    WHERE evt_block_time >= '2023-01-01'
        AND evt_block_time < '2023-02-01'
    
    UNION ALL

    SELECT evt_block_time as raw_block_time,
        evt_tx_hash as raw_tx_hash,
        evt_tx_hash || '-Trade-' || evt_index || '-' || erc1155TypeId AS raw_unique_trade_id
    from {{ source ('aavegotchi_polygon', 'aavegotchi_diamond_evt_ERC1155ExecutedListing') }}
    WHERE evt_block_time >= '2023-01-01'
        AND evt_block_time < '2023-02-01'
),

processed_events AS (
    SELECT block_time,
        tx_hash,
        unique_trade_id
    FROM {{ ref('aavegotchi_polygon_events') }}
    WHERE block_time >= '2023-01-01'
        AND block_time < '2023-02-01'
        AND evt_type = 'Trade'
)

SELECT *
FROM raw_events r
FULL JOIN processed_events n ON r.raw_block_time = n.block_time AND r.raw_unique_trade_id = n.unique_trade_id
WHERE r.raw_unique_trade_id IS NULL 
    Or n.unique_trade_id IS NULL
