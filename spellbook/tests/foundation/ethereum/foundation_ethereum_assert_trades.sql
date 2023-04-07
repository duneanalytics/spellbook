-- Check if all Ethereum Foundation trade events make it into the nft.trades
WITH raw_events AS (
  SELECT f.evt_block_time AS raw_block_time
  , f.evt_tx_hash AS raw_tx_hash
  , c.nftContract AS raw_nft_contract_address
  , c.tokenId AS raw_token_id
  , f.evt_tx_hash || '-' || c.nftContract || '-' || c.tokenId AS raw_unique_trade_id
  FROM {{ source('foundation_ethereum','market_evt_ReserveAuctionFinalized') }} f
  LEFT JOIN {{ source('foundation_ethereum','market_evt_ReserveAuctionCreated') }} c ON c.auctionId=f.auctionId AND c.evt_block_time<=f.evt_block_time
  WHERE f.evt_block_time >= now() - interval 7 day
  AND f.evt_block_time < NOW() - interval '1 day' -- allow some head desync
  UNION
  SELECT f.evt_block_time AS raw_block_time
  , f.evt_tx_hash AS raw_tx_hash
  , f.nftContract AS raw_nft_contract_address
  , f.tokenId AS raw_token_id
  , f.evt_tx_hash || '-' || f.nftContract || '-' || f.tokenId AS raw_unique_trade_id
    FROM {{ source('foundation_ethereum','market_evt_BuyPriceAccepted') }} f
  WHERE f.evt_block_time >= now() - interval 7 day
  AND f.evt_block_time < NOW() - interval '1 day' -- allow some head desync
  UNION
  SELECT f.evt_block_time AS raw_block_time
  , f.evt_tx_hash AS raw_tx_hash
  , f.nftContract AS raw_nft_contract_address
  , f.tokenId AS raw_token_id
  , f.evt_tx_hash || '-' || f.nftContract || '-' || f.tokenId AS raw_unique_trade_id
    FROM {{ source('foundation_ethereum','market_evt_OfferAccepted') }} f
  WHERE f.evt_block_time >= '2022-04-15'
  AND f.evt_block_time < NOW() - interval '1 day' -- allow some head desync
  UNION
  SELECT f.evt_block_time AS raw_block_time
  , f.evt_tx_hash AS raw_tx_hash
  , f.nftContract AS raw_nft_contract_address
  , f.tokenId AS raw_token_id
  , f.evt_tx_hash || '-' || f.nftContract || '-' || f.tokenId AS raw_unique_trade_id
    FROM {{ source('foundation_ethereum','market_evt_PrivateSaleFinalized') }} f
  WHERE f.evt_block_time >= now() - interval 7 day
  AND f.evt_block_time < NOW() - interval '1 day' -- allow some head desync
  )

, processed_events AS (
  SELECT block_time AS processed_block_time
  , tx_hash AS processed_tx_hash
  , nft_contract_address AS processed_nft_contract_address
  , token_id AS processed_token_id
  , tx_hash || '-' || nft_contract_address || '-' || token_id AS processed_trade_id
  FROM {{ ref('foundation_ethereum_events') }}
  WHERE blockchain = 'ethereum'
    AND project = 'foundation'
    AND version = 'v1'
    AND block_time >= now() - interval 7 day
    AND block_time < NOW() - interval '1 day' -- allow some head desync
  )

SELECT *
FROM raw_events
OUTER JOIN processed_events n ON raw_block_time = processed_block_time
  AND raw_unique_trade_id = processed_trade_id
WHERE NOT raw_unique_trade_id = processed_trade_id
