-- Check if all Avalanche Element trade events make it into the nft.trades
WITH raw_events AS (
  SELECT evt_block_time AS raw_block_time
  , evt_tx_hash AS raw_tx_hash
  , erc721Token AS raw_nft_contract_address
  , erc721TokenId AS raw_token_id
  , evt_tx_hash || erc721Token || erc721TokenId AS raw_unique_trade_id
  FROM {{ source('element_ex_avalanche_c','OrdersFeature_evt_ERC721SellOrderFilled') }}
  WHERE evt_block_time >= '2022-04-15'
  AND evt_block_time < NOW() - interval '1 day' -- allow some head desync
  UNION
  SELECT evt_block_time AS raw_block_time
  , evt_tx_hash AS raw_tx_hash
  , erc721Token AS raw_nft_contract_address
  , erc721TokenId AS raw_token_id
  , evt_tx_hash || erc721Token || erc721TokenId AS raw_unique_trade_id
  FROM {{ source('element_ex_avalanche_c','OrdersFeature_evt_ERC721BuyOrderFilled') }}
  WHERE evt_block_time >= '2022-04-15'
  AND evt_block_time < NOW() - interval '1 day' -- allow some head desync
  UNION
  SELECT evt_block_time AS raw_block_time
  , evt_tx_hash AS raw_tx_hash
  , erc1155Token AS raw_nft_contract_address
  , erc1155TokenId AS raw_token_id
  , evt_tx_hash || erc1155Token || erc1155TokenId AS raw_unique_trade_id
  FROM {{ source('element_ex_avalanche_c','OrdersFeature_evt_ERC1155SellOrderFilled') }}
  WHERE evt_block_time >= '2022-04-15'
  AND evt_block_time < NOW() - interval '1 day' -- allow some head desync
  UNION
  SELECT evt_block_time AS raw_block_time
  , evt_tx_hash AS raw_tx_hash
  , erc1155Token AS raw_nft_contract_address
  , erc1155TokenId AS raw_token_id
  , evt_tx_hash || erc1155Token || erc1155TokenId AS raw_unique_trade_id
  FROM {{ source('element_ex_avalanche_c','OrdersFeature_evt_ERC1155BuyOrderFilled') }}
  WHERE evt_block_time >= '2022-04-15'
  AND evt_block_time < NOW() - interval '1 day' -- allow some head desync
  )

, processed_events AS (
  SELECT block_time AS processed_block_time
  , tx_hash AS processed_tx_hash
  , nft_contract_address AS processed_nft_contract_address
  , token_id AS processed_token_id
  , tx_hash || nft_contract_address || token_id AS processed_unique_trade_id
  FROM {{ ref('element_avalanche_c_events') }}
  WHERE blockchain = 'avalanche_c'
    AND project = 'element'
    AND version = 'v1'
    AND block_time >= '2022-04-15'
    AND block_time < NOW() - interval '1 day' -- allow some head desync
  )

SELECT *
FROM raw_events
OUTER JOIN processed_events n ON raw_block_time = processed_block_time
  AND raw_unique_trade_id = processed_unique_trade_id
WHERE NOT raw_unique_trade_id = processed_unique_trade_id
