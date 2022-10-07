-- Check if all Ethereum Zora trades events make it into the zora_ethereum.trades
WITH raw_events AS (
  SELECT evt_block_time AS raw_block_time
  , evt_tx_hash AS raw_tx_hash
  , get_json_object(a, '$.tokenContract') AS raw_nft_contract_address
  , get_json_object(a, '$.tokenId') AS raw_token_id
  , evt_tx_hash || get_json_object(a, '$.tokenContract') || get_json_object(a, '$.tokenId') AS raw_unique_trade_id
  FROM {{ source('zora_v3_ethereum','OffersV1_evt_ExchangeExecuted') }}
  WHERE evt_block_time >= '2021-01-30'
  AND evt_block_time < NOW() - interval '1 day' -- allow some head desync
  UNION
  SELECT evt_block_time AS raw_block_time
  , evt_tx_hash AS raw_tx_hash
  , get_json_object(a, '$.tokenContract') AS raw_nft_contract_address
  , get_json_object(a, '$.tokenId') AS raw_token_id
  , evt_tx_hash || get_json_object(a, '$.tokenContract') || get_json_object(a, '$.tokenId') AS raw_unique_trade_id
  FROM {{ source('zora_v3_ethereum','AsksV1_0_evt_ExchangeExecuted') }}
  WHERE evt_block_time >= '2021-01-30'
  AND evt_block_time < NOW() - interval '1 day' -- allow some head desync
  UNION
  SELECT evt_block_time AS raw_block_time
  , evt_tx_hash AS raw_tx_hash
  , get_json_object(a, '$.tokenContract') AS raw_nft_contract_address
  , get_json_object(a, '$.tokenId') AS raw_token_id
  , evt_tx_hash || get_json_object(a, '$.tokenContract') || get_json_object(a, '$.tokenId') AS raw_unique_trade_id
  FROM {{ source('zora_v3_ethereum','AsksV1_1_evt_ExchangeExecuted') }}
  WHERE evt_block_time >= '2021-01-30'
  AND evt_block_time < NOW() - interval '1 day' -- allow some head desync
  UNION
  SELECT evt_block_time AS raw_block_time
  , evt_tx_hash AS raw_tx_hash
  , tokenContract AS raw_nft_contract_address
  , tokenId AS raw_token_id
  , evt_tx_hash || tokenContract || tokenId AS raw_unique_trade_id
  FROM {{ source('zora_v3_ethereum','ReserveAuctionFindersEth_evt_AuctionEnded') }}
  WHERE evt_block_time >= '2021-01-30'
  AND evt_block_time < NOW() - interval '1 day' -- allow some head desync
  UNION
  SELECT evt_block_time AS raw_block_time
  , evt_tx_hash AS raw_tx_hash
  , tokenContract AS raw_nft_contract_address
  , tokenId AS raw_token_id
  , evt_tx_hash || tokenContract || tokenId AS raw_unique_trade_id
  FROM {{ source('zora_v3_ethereum','AsksPrivateEth_evt_AskFilled') }}
  WHERE evt_block_time >= '2021-01-30'
  AND evt_block_time < NOW() - interval '1 day' -- allow some head desync
  UNION
  SELECT evt_block_time AS raw_block_time
  , evt_tx_hash AS raw_tx_hash
  , tokenContract AS raw_nft_contract_address
  , tokenId AS raw_token_id
  , evt_tx_hash || tokenContract || tokenId AS raw_unique_trade_id
  FROM {{ source('zora_v3_ethereum','AsksCoreEth_evt_AskFilled') }}
  WHERE evt_block_time >= '2021-01-30'
  AND evt_block_time < NOW() - interval '1 day' -- allow some head desync
  UNION
  SELECT evt_block_time AS raw_block_time
  , evt_tx_hash AS raw_tx_hash
  , tokenContract AS raw_nft_contract_address
  , tokenId AS raw_token_id
  , evt_tx_hash || tokenContract || tokenId AS raw_unique_trade_id
  FROM {{ source('zora_v3_ethereum','ReserveAuctionCoreEth_evt_AuctionEnded') }}
  WHERE evt_block_time >= '2021-01-30'
  AND evt_block_time < NOW() - interval '1 day' -- allow some head desync
  UNION
  SELECT evt_block_time AS raw_block_time
  , evt_tx_hash AS raw_tx_hash
  , tokenContract AS raw_nft_contract_address
  , tokenId AS raw_token_id
  , evt_tx_hash || tokenContract || tokenId AS raw_unique_trade_id
  FROM {{ source('zora_v3_ethereum','ReserveAuctionCoreErc20_evt_AuctionEnded') }}
  WHERE evt_block_time >= '2021-01-30'
  AND evt_block_time < NOW() - interval '1 day' -- allow some head desync
  UNION
  SELECT evt_block_time AS raw_block_time
  , evt_tx_hash AS raw_tx_hash
  , tokenContract AS raw_nft_contract_address
  , tokenId AS raw_token_id
  , evt_tx_hash || tokenContract || tokenId AS raw_unique_trade_id
  FROM {{ source('zora_v3_ethereum','ReserveAuctionFindersErc20_evt_AuctionEnded') }}
  WHERE evt_block_time >= '2021-01-30'
  AND evt_block_time < NOW() - interval '1 day' -- allow some head desync
  UNION
  SELECT evt_block_time AS raw_block_time
  , evt_tx_hash AS raw_tx_hash
  , tokenContract AS raw_nft_contract_address
  , tokenId AS raw_token_id
  , evt_tx_hash || tokenContract || tokenId AS raw_unique_trade_id
  FROM {{ source('zora_v3_ethereum','ReserveAuctionListingEth_evt_AuctionEnded') }}
  WHERE evt_block_time >= '2021-01-30'
  AND evt_block_time < NOW() - interval '1 day' -- allow some head desync
  UNION
  SELECT evt_block_time AS raw_block_time
  , evt_tx_hash AS raw_tx_hash
  , tokenContract AS raw_nft_contract_address
  , tokenId AS raw_token_id
  , evt_tx_hash || tokenContract || tokenId AS raw_unique_trade_id
  FROM {{ source('zora_v3_ethereum','ReserveAuctionListingErc20_evt_AuctionEnded') }}
  WHERE evt_block_time >= '2021-01-30'
  AND evt_block_time < NOW() - interval '1 day' -- allow some head desync
  UNION
  SELECT evt_block_time AS raw_block_time
  , evt_tx_hash AS raw_tx_hash
  , tokenContract AS raw_nft_contract_address
  , tokenId AS raw_token_id
  , evt_tx_hash || tokenContract || tokenId AS raw_unique_trade_id
  FROM {{ source('zora_ethereum','AuctionHouse_evt_AuctionEnded') }}
  WHERE evt_block_time >= '2021-01-30'
  AND evt_block_time < NOW() - interval '1 day' -- allow some head desync
  UNION
  SELECT evt_block_time AS raw_block_time
  , evt_tx_hash AS raw_tx_hash
  , '0xabefbc9fd2f806065b4f3c237d4b59d9a97bcac7' AS raw_nft_contract_address
  , tokenId AS raw_token_id
  , evt_tx_hash || '0xabefbc9fd2f806065b4f3c237d4b59d9a97bcac7' || tokenId AS raw_unique_trade_id
  FROM {{ source('zora_ethereum','Market_evt_BidFinalized') }}
  WHERE evt_block_time >= '2021-01-30'
  AND evt_block_time < NOW() - interval '1 day' -- allow some head desync
  )

, processed_events AS (
  SELECT block_time AS processed_block_time
  , tx_hash AS processed_tx_hash
  , nft_contract_address AS processed_nft_contract_address
  , token_id AS processed_token_id
  , tx_hash || nft_contract_address || token_id AS processed_unique_trade_id
  FROM {{ ref('zora_ethereum_events') }}
  WHERE blockchain = 'ethereum'
    AND project = 'zora'
    AND version IN ('v1', 'v2', 'v3')
    AND block_time >= '2021-01-30'
    AND block_time < NOW() - interval '1 day' -- allow some head desync
  )

SELECT *
FROM raw_events
OUTER JOIN processed_events n ON raw_block_time = processed_block_time
  AND raw_unique_trade_id = processed_unique_trade_id
WHERE NOT raw_unique_trade_id = processed_unique_trade_id
