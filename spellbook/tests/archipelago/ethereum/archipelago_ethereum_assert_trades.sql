-- Check if all archipelago trade events make it into the nft.trades
WITH raw_events AS (
    SELECT
            evt_block_time as raw_block_time
            , evt_tx_hash as raw_tx_hash
            , tradeId as raw_unique_trade_id
        FROM {{ source('archipelago_ethereum','ArchipelagoMarket_evt_Trade') }}
        WHERE evt_block_time >= '2022-6-20'
        AND evt_block_time < now() - interval '1 day' -- allow some head desync
)

, processed_events AS (
    SELECT
      block_time as processed_block_time
      , tx_hash as processed_tx_hash
      , unique_trade_id as processed_trade_id
    FROM {{ ref('archipelago_ethereum_events') }}
    WHERE
      blockchain = 'ethereum'
      AND project = 'archipelago'
      and version = 'v1'
      AND block_time >= '2022-6-20'
      AND block_time < now() - interval '1 day' -- allow some head desync
)

SELECT
    *
    from raw_events
    outer join processed_events n
        ON raw_block_time = processed_block_time AND raw_unique_trade_id = processed_trade_id
    where not raw_unique_trade_id = processed_trade_id
