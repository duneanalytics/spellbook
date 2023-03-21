-- Check if all fractal events make it into the processed events table
with listing_detail AS (
    SELECT assetContract AS nft_contract_address,
        contract_address,
        evt_block_number,
        evt_block_time,
        evt_index,
        evt_tx_hash,
        lister,
        listingId,
        listing:tokenOwner AS tokenOwner,
        listing:tokenId AS tokenId,
        listing:startTime AS startTime,
        listing:endTime AS endTime,
        listing:quantity AS quantity,
        listing:currency AS currency,
        listing:reservePricePerToken AS reservePricePerToken,
        listing:buyoutPricePerToken AS buyoutPricePerToken,
        listing:tokenType AS tokenType,
        listing:listingType AS listingType
    FROM {{ source ('fractal_polygon', 'Marketplace_evt_ListingAdded') }}
),

raw_events AS (
    SELECT s.evt_block_time as raw_block_time,
        s.evt_tx_hash as raw_tx_hash,
        s.evt_tx_hash || '-Trade-' || s.evt_index || '-' || l.tokenId AS raw_unique_trade_id
    FROM {{ source ('fractal_polygon', 'Marketplace_evt_NewSale') }} s
    INNER JOIN listing_detail l ON s.listingId = l.listingId
    WHERE s.evt_block_time >= '2023-01-01'
        AND s.evt_block_time < '2023-02-01'
),

processed_events AS (
    SELECT block_time,
        tx_hash,
        unique_trade_id
    FROM {{ ref('fractal_polygon_events') }}
    WHERE block_time >= '2023-01-01'
        AND block_time < '2023-02-01'
        AND evt_type = 'Trade'
)

SELECT *
FROM raw_events r
FULL JOIN processed_events n ON r.raw_block_time = n.block_time AND r.raw_unique_trade_id = n.unique_trade_id
WHERE r.raw_unique_trade_id IS NULL 
    Or n.unique_trade_id IS NULL
