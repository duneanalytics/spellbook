-- Check if all minting txs are in nft_ethereum_native_mints
WITH raw_events AS (
  SELECT evt_tx_hash AS raw_tx_hash
  FROM {{ source('erc721_ethereum','evt_transfer') }}
  WHERE from='0x0000000000000000000000000000000000000000'
  AND evt_block_time < NOW() - interval '1 day' -- allow some head desync
  UNION
  SELECT evt_tx_hash AS raw_tx_hash
  FROM {{ source('erc1155_ethereum','evt_transfersingle') }}
  WHERE from='0x0000000000000000000000000000000000000000'
  AND evt_block_time < NOW() - interval '1 day' -- allow some head desync
  UNION
  SELECT evt_tx_hash AS raw_tx_hash
  FROM {{ source('erc1155_ethereum','evt_transferbatch') }}
  WHERE from='0x0000000000000000000000000000000000000000'
  AND evt_block_time < NOW() - interval '1 day' -- allow some head desync
  )

, processed_events AS (
  SELECT tx_hash AS processed_tx_hash
  FROM {{ ref('nft_ethereum_native_mints') }}
  )

SELECT *
FROM raw_events
OUTER JOIN processed_events n ON raw_tx_hash = processed_tx_hash
WHERE NOT raw_tx_hash = processed_tx_hash
