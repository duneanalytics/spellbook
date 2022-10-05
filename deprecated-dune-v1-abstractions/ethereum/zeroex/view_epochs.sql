CREATE OR REPLACE VIEW zeroex.view_epochs AS (
WITH
    past_epochs AS (
        SELECT
            ee.epoch AS epoch_id
            , ee."rewardsAvailable"::NUMERIC / 1e18 AS rewards_available_in_eth
            , ee."totalFeesCollected"::NUMERIC / 1e18 AS total_fees_collected_by_pools_in_eth
            -- fill in epoch 1 starting values with the staking proxy deployment tx
            , CASE
                    WHEN ee.epoch = 1 THEN '4680e9d59bae9bbde1b0bae0fa5157ceea64ea923f2be434e5da6f5df2bdb907'::bytea
                    ELSE LAG(ee.evt_tx_hash) OVER (ORDER BY ee.epoch)
                END AS starting_transaction_hash
            , CASE
                    WHEN ee.epoch = 1 THEN 43
                    ELSE LAG(tx.index) OVER (ORDER BY ee.epoch)
                END AS starting_transaction_index
            , CASE
                    WHEN ee.epoch = 1 THEN 8952581
                    ELSE LAG(ee.evt_block_number) OVER (ORDER BY ee.epoch)
                END AS starting_block_number
            , CASE
                    WHEN ee.epoch = 1 THEN '2019-11-17 20:58:01.000000 +00:00'::TIMESTAMP
                    ELSE LAG(b.time) OVER (ORDER BY ee.epoch)
                END AS starting_block_timestamp
            , evt_tx_hash AS ending_transaction_hash
            , tx.index AS ending_transaction_index
            , ee.evt_block_number AS ending_block_number
            , b.time AS ending_block_timestamp
        FROM zeroex_v3."StakingProxy_evt_EpochEnded" ee
        LEFT JOIN ethereum.blocks b ON b.number = ee.evt_block_number
        LEFT JOIN ethereum.transactions tx ON tx.hash = ee.evt_tx_hash
        ORDER BY 1
    )
    , current_epoch AS (
        SELECT
            epoch + 1 AS epoch_id
            , NULL::NUMERIC AS rewards_available_in_eth
            , NULL::NUMERIC AS total_fees_collected_by_pools_in_eth
            , evt_tx_hash AS starting_transaction_hash
            , tx.index AS starting_transaction_index
            , ee.evt_block_number AS starting_block_number
            , b.time AS starting_block_timestamp
            , NULL::BYTEA AS ending_transaction_hash
            , NULL::BIGINT AS ending_transaction_index
            , NULL::BIGINT AS ending_block_number
            , NULL::TIMESTAMPTZ AS ending_block_timestamp
        FROM zeroex_v3."StakingProxy_evt_EpochEnded" ee
        LEFT JOIN ethereum.blocks b ON b.number = ee.evt_block_number
        LEFT JOIN ethereum.transactions tx ON tx.hash = ee.evt_tx_hash
        ORDER BY epoch_id DESC
        LIMIT 1
    )
    SELECT * FROM past_epochs

    UNION ALL

    SELECT * FROM current_epoch
);
