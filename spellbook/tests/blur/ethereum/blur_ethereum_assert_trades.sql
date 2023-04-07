-- Check if all Ethereum Blur trade events make it into the nft.trades
WITH raw_events AS (
  SELECT bm.evt_block_time AS raw_block_time
  , bm.evt_tx_hash AS raw_tx_hash
  , get_json_object(bm.buy, '$.collection') AS raw_nft_contract_address
  , get_json_object(bm.buy, '$.tokenId') AS raw_token_id
  , bm.evt_tx_hash || '-' || get_json_object(bm.buy, '$.collection') || '-' || get_json_object(bm.buy, '$.tokenId') AS raw_unique_trade_id
  FROM {{ source('blur_ethereum','BlurExchange_evt_OrdersMatched') }} bm
  WHERE bm.evt_block_time >= '2022-10-19'
  AND bm.evt_block_time < NOW() - interval '1 day' -- allow some head desync
  )

, processed_events AS (
  SELECT block_time AS processed_block_time
  , tx_hash AS processed_tx_hash
  , nft_contract_address AS processed_nft_contract_address
  , token_id AS processed_token_id
  , tx_hash || '-' || nft_contract_address || '-' || token_id AS processed_trade_id
  FROM {{ ref('blur_ethereum_events') }}
  WHERE blockchain = 'ethereum'
    AND project = 'blur'
    AND version = 'v1'
    AND block_time >= '2022-10-19'
    AND block_time < NOW() - interval '1 day' -- allow some head desync
  )

SELECT *
FROM raw_events
OUTER JOIN processed_events n ON raw_block_time = processed_block_time
  AND raw_unique_trade_id = processed_trade_id
WHERE NOT raw_unique_trade_id = processed_trade_id
