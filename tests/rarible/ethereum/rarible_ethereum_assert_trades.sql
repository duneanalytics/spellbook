-- Check if all Ethereum Rarible trades events make it into the zora_ethereum.trades
WITH raw_events AS (
  SELECT evt_block_time AS raw_block_time
  , evt_tx_hash AS raw_tx_hash
  , token AS raw_nft_contract_address
  , tokenId AS raw_token_id
  , evt_tx_hash || token || tokenId AS raw_unique_trade_id
  FROM {{ source('rarible_ethereum','TokenSale_evt_Buy') }}
  AND evt_block_time < NOW() - interval '1 day' -- allow some head desync
  UNION
  SELECT evt_block_time AS raw_block_time
  , evt_tx_hash AS raw_tx_hash
  , token AS raw_nft_contract_address
  , tokenId AS raw_token_id
  , evt_tx_hash || token || tokenId AS raw_unique_trade_id
  FROM {{ source('rarible_v1_ethereum','ERC1155Sale_v1_evt_Buy') }}
  AND evt_block_time < NOW() - interval '1 day' -- allow some head desync
  UNION
  SELECT evt_block_time AS raw_block_time
  , evt_tx_hash AS raw_tx_hash
  , token AS raw_nft_contract_address
  , tokenId AS raw_token_id
  , evt_tx_hash || token || tokenId AS raw_unique_trade_id
  FROM {{ source('rarible_v1_ethereum','ERC721Sale_v1_evt_Buy') }}
  AND evt_block_time < NOW() - interval '1 day' -- allow some head desync
  UNION
  SELECT evt_block_time AS raw_block_time
  , evt_tx_hash AS raw_tx_hash
  , token AS raw_nft_contract_address
  , tokenId AS raw_token_id
  , evt_tx_hash || token || tokenId AS raw_unique_trade_id
  FROM {{ source('rarible_v1_ethereum','ERC721Sale_v2_evt_Buy') }}
  AND evt_block_time < NOW() - interval '1 day' -- allow some head desync
  UNION
  SELECT evt_block_time AS raw_block_time
  , evt_tx_hash AS raw_tx_hash
  , token AS raw_nft_contract_address
  , tokenId AS raw_token_id
  , evt_tx_hash || token || tokenId AS raw_unique_trade_id
  FROM  {{ source('rarible_v1_ethereum','ERC1155Sale_v2_evt_Buy') }}
  AND evt_block_time < NOW() - interval '1 day' -- allow some head desync
  UNION
  SELECT evt_block_time AS raw_block_time
  , evt_tx_hash AS raw_tx_hash
  , sellToken AS raw_nft_contract_address
  , selltokenId AS raw_token_id
  , evt_tx_hash || sellToken || selltokenId AS raw_unique_trade_id
  FROM {{ source('rarible_ethereum','ExchangeV1_evt_Buy') }}
  AND evt_block_time < NOW() - interval '1 day' -- allow some head desync
  UNION
  SELECT evt_block_time AS raw_block_time
  , evt_tx_hash AS raw_tx_hash
  , '0x' || substring(get_json_object(s.leftAsset, '$.data'), 27, 40) AS raw_nft_contract_address
  , ROUND(bytea2numeric_v2(substring(get_json_object(s.leftAsset, '$.data'), 67, 64)), 0) AS raw_token_id
  , evt_tx_hash || '0x' || substring(get_json_object(s.leftAsset, '$.data'), 27, 40) || ROUND(bytea2numeric_v2(substring(get_json_object(s.leftAsset, '$.data'), 67, 64)), 0) AS raw_unique_trade_id
  FROM {{ source('rarible_ethereum','ExchangeV2_evt_Match') }}
  AND evt_block_time < NOW() - interval '1 day' -- allow some head desync
  UNION
  SELECT evt_block_time AS raw_block_time
  , evt_tx_hash AS raw_tx_hash
  , '0x' || substring(get_json_object(s.rightAsset, '$.data'), 27, 40) AS raw_nft_contract_address
  , ROUND(bytea2numeric_v2(substring(get_json_object(s.leftAsset, '$.data'), 67, 64)), 0) AS raw_token_id
  , evt_tx_hash || '0x' || substring(get_json_object(s.rightAsset, '$.data'), 27, 40) || ROUND(bytea2numeric_v2(substring(get_json_object(s.leftAsset, '$.data'), 67, 64)), 0) AS raw_unique_trade_id
  FROM {{ source('rarible_ethereum','ExchangeV2_evt_Match') }}
  AND evt_block_time < NOW() - interval '1 day' -- allow some head desync
  UNION
  SELECT evt_block_time AS raw_block_time
  , evt_tx_hash AS raw_tx_hash
  , '0x' || substring(get_json_object(s.rightAsset, '$.data'), 27, 40) AS raw_nft_contract_address
  , ROUND(bytea2numeric_v2(substring(get_json_object(s.leftAsset, '$.data'), 67, 64)), 0) AS raw_token_id
  , evt_tx_hash || '0x' || substring(get_json_object(s.rightAsset, '$.data'), 27, 40) || ROUND(bytea2numeric_v2(substring(get_json_object(s.leftAsset, '$.data'), 67, 64)), 0) AS raw_unique_trade_id
  FROM {{ source('rarible_ethereum','ExchangeV2_evt_Match') }}
  AND evt_block_time < NOW() - interval '1 day' -- allow some head desync
  )

, processed_events AS (
  SELECT block_time AS processed_block_time
  , tx_hash AS processed_tx_hash
  , nft_contract_address AS processed_nft_contract_address
  , token_id AS processed_token_id
  , tx_hash || nft_contract_address || token_id AS processed_unique_trade_id
  FROM {{ ref('rarible_ethereum_events') }}
  WHERE blockchain = 'ethereum'
    AND project = 'rarible'
    AND version IN ('v1', 'v2')
    AND block_time < NOW() - interval '1 day' -- allow some head desync
  )

SELECT *
FROM raw_events
OUTER JOIN processed_events n ON raw_block_time = processed_block_time
  AND raw_unique_trade_id = processed_unique_trade_id
WHERE NOT raw_unique_trade_id = processed_unique_trade_id
