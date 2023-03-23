-- Check if all fractal events make it into the processed events table
WITH raw_events AS (
    select evt_block_time as raw_block_time,
        evt_tx_hash as raw_tx_hash,
        CAST(bytea2numeric_v3(substr(leftAsset:data, 3 + 64, 64)) AS string) AS raw_token_id,
        evt_tx_hash || '-Trade-' || evt_index || '-' || CAST(bytea2numeric_v3(substr(leftAsset:data, 3 + 64, 64)) AS string) || '-' || newRightFill AS raw_unique_trade_id
    FROM {{ source ('rarible_polygon', 'Exchange_evt_Match') }}
    WHERE rightAsset:assetClass in ('0xaaaebeba', '0x8ae85d84')
        AND evt_block_time >= '2022-04-01'
        AND evt_block_time < '2022-05-01'

    UNION ALL

    select evt_block_time as raw_block_time,
        evt_tx_hash as raw_tx_hash,
        CAST(bytea2numeric_v3(substr(rightAsset:data, 3 + 64, 64)) AS string) AS token_id,
        evt_tx_hash || '-Trade-' || evt_index || '-' || CAST(bytea2numeric_v3(substr(rightAsset:data, 3 + 64, 64)) AS string) || '-' || newLeftFill  AS raw_unique_trade_id
    FROM {{ source ('rarible_polygon', 'Exchange_evt_Match') }}
    WHERE leftAsset:assetClass in ('0xaaaebeba', '0x8ae85d84')
        AND evt_block_time >= '2022-04-01'
        AND evt_block_time < '2022-05-01'
),

processed_events AS (
    SELECT block_time,
        tx_hash,
        unique_trade_id
    FROM {{ ref('rarible_polygon_events') }}
    WHERE block_time >= '2022-04-01'
        AND block_time < '2022-05-01'
        AND evt_type = 'Trade'
)

SELECT *
FROM raw_events r
FULL JOIN processed_events n ON r.raw_block_time = n.block_time AND r.raw_unique_trade_id = n.unique_trade_id
WHERE r.raw_unique_trade_id IS NULL 
    Or n.unique_trade_id IS NULL
