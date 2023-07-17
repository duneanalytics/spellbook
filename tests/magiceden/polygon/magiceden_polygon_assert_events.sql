-- Check if all fractal events make it into the processed events table
with raw_events AS (
    SELECT evt_block_time as raw_block_time,
          evt_tx_hash as raw_tx_hash,
          evt_tx_hash || '-Trade-' || evt_index || '-' || erc721TokenId || '-1'  AS raw_unique_trade_id
    FROM {{ source ('zeroex_polygon', 'ExchangeProxy_evt_ERC721OrderFilled') }}
    WHERE substring(nonce, 1, 38) = '10013141590000000000000000000000000000'
        AND evt_block_time >= '2023-01-01'
        AND evt_block_time < '2023-02-01'

    UNION ALL

    SELECT evt_block_time as raw_block_time,
          evt_tx_hash as raw_tx_hash,
          evt_tx_hash || '-Trade-' || evt_index || '-' || erc1155TokenId || '-' || erc1155FillAmount AS raw_unique_trade_id
    FROM {{ source ('zeroex_polygon', 'ExchangeProxy_evt_ERC1155OrderFilled') }}
    WHERE substring(nonce, 1, 38) = '10013141590000000000000000000000000000'
        AND evt_block_time >= '2023-01-01'
        AND evt_block_time < '2023-02-01'
),

processed_events AS (
    SELECT block_time,
        tx_hash,
        unique_trade_id
    FROM {{ ref('magiceden_polygon_events') }}
    WHERE block_time >= '2023-01-01'
        AND block_time < '2023-02-01'
        AND evt_type = 'Trade'
)

SELECT *
FROM raw_events r
FULL JOIN processed_events n ON r.raw_block_time = n.block_time AND r.raw_unique_trade_id = n.unique_trade_id
WHERE r.raw_unique_trade_id IS NULL 
    Or n.unique_trade_id IS NULL
