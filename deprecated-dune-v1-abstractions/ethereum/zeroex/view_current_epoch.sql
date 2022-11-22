CREATE OR REPLACE VIEW zeroex.view_current_epoch AS (
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
);
