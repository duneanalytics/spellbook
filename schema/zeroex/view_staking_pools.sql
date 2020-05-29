CREATE OR REPLACE VIEW zeroex.view_staking_pools AS (
    SELECT
        HEX_TO_INT(RIGHT(spc."poolId"::VARCHAR,5)) AS pool_id
        , spc.operator
        , spc.evt_block_number AS created_at_block_number
        , spc.evt_block_time AS created_at_timestamp
        , spc.evt_tx_hash AS created_at_transaction_hash
        , tx.index AS created_at_transaction_index
    FROM zeroex_v3."StakingProxy_evt_StakingPoolCreated" spc
    LEFT JOIN ethereum.transactions tx ON tx.hash = spc.evt_tx_hash
    ORDER BY 1
);
